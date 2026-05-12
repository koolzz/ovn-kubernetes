// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

func gwState(entries map[string]bool) *desiredGWState {
	d := newDesiredGWState()
	for ip, bfd := range entries {
		d.addGW(ip, bfd)
	}
	return d
}

func TestComputeGWStateDelta_Empty(t *testing.T) {
	cases := []struct {
		name             string
		applied, desired *desiredGWState
	}{
		{"both nil", nil, nil},
		{"both empty", newDesiredGWState(), newDesiredGWState()},
		{"applied nil, desired empty", nil, newDesiredGWState()},
		{"applied empty, desired nil", newDesiredGWState(), nil},
		{"identical single", gwState(map[string]bool{"10.0.0.1": true}), gwState(map[string]bool{"10.0.0.1": true})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := computeGWStateDelta(tc.applied, tc.desired)
			if !d.empty() {
				t.Fatalf("expected empty delta, got add=%v remove=%v replace=%v", d.add, d.remove, d.replace)
			}
		})
	}
}

func TestComputeGWStateDelta_PureAdd(t *testing.T) {
	applied := newDesiredGWState()
	desired := gwState(map[string]bool{
		"10.0.0.2": false,
		"10.0.0.1": true,
	})
	d := computeGWStateDelta(applied, desired)
	want := gwStateDelta{add: []gwIPBFD{{"10.0.0.1", true}, {"10.0.0.2", false}}}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("delta mismatch\n got: %+v\nwant: %+v", d, want)
	}
}

func TestComputeGWStateDelta_PureRemove(t *testing.T) {
	applied := gwState(map[string]bool{
		"10.0.0.2": false,
		"10.0.0.1": true,
	})
	desired := newDesiredGWState()
	d := computeGWStateDelta(applied, desired)
	want := gwStateDelta{remove: []string{"10.0.0.1", "10.0.0.2"}}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("delta mismatch\n got: %+v\nwant: %+v", d, want)
	}
}

func TestComputeGWStateDelta_BFDReplace(t *testing.T) {
	applied := gwState(map[string]bool{"10.0.0.1": false})
	desired := gwState(map[string]bool{"10.0.0.1": true})
	d := computeGWStateDelta(applied, desired)
	want := gwStateDelta{replace: []gwIPBFD{{"10.0.0.1", true}}}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("delta mismatch\n got: %+v\nwant: %+v", d, want)
	}
}

func TestComputeGWStateDelta_BFDReplace_TrueToFalse(t *testing.T) {
	// The migration plan emphasizes BFD-flip-with-same-IP must not be
	// silently a no-op. Verify the false direction is also a replace.
	applied := gwState(map[string]bool{"10.0.0.1": true})
	desired := gwState(map[string]bool{"10.0.0.1": false})
	d := computeGWStateDelta(applied, desired)
	want := gwStateDelta{replace: []gwIPBFD{{"10.0.0.1", false}}}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("delta mismatch\n got: %+v\nwant: %+v", d, want)
	}
}

func TestComputeGWStateDelta_Mixed(t *testing.T) {
	applied := gwState(map[string]bool{
		"10.0.0.1": true,  // unchanged
		"10.0.0.2": false, // BFD flip → replace
		"10.0.0.3": true,  // gone → remove
	})
	desired := gwState(map[string]bool{
		"10.0.0.1": true,  // unchanged
		"10.0.0.2": true,  // BFD flipped
		"10.0.0.4": false, // new → add
		"10.0.0.5": true,  // new → add
	})
	d := computeGWStateDelta(applied, desired)
	want := gwStateDelta{
		add:     []gwIPBFD{{"10.0.0.4", false}, {"10.0.0.5", true}},
		remove:  []string{"10.0.0.3"},
		replace: []gwIPBFD{{"10.0.0.2", true}},
	}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("delta mismatch\n got: %+v\nwant: %+v", d, want)
	}
}

func TestComputeGWStateDelta_AppliedNil_RecordsAllAsAdds(t *testing.T) {
	// Bootstrap edge: no applied state recorded for the namespace at
	// all. Every desired IP is an add. (Distinct from applied=empty,
	// which is the same observable behavior here, but the cache
	// distinguishes "never seen" from "seen empty" — see Has().)
	desired := gwState(map[string]bool{"10.0.0.1": true})
	d := computeGWStateDelta(nil, desired)
	if len(d.add) != 1 || d.add[0] != (gwIPBFD{"10.0.0.1", true}) {
		t.Fatalf("expected single add for 10.0.0.1; got %+v", d)
	}
}

func TestNSAppliedGWState_GetSetDelete(t *testing.T) {
	s := newNSAppliedGWState()
	if got := s.Get("ns1"); got != nil {
		t.Fatalf("Get on empty cache should return nil; got %v", got)
	}
	state := gwState(map[string]bool{"10.0.0.1": true})
	s.Set("ns1", state)
	if got := s.Get("ns1"); got != state {
		t.Fatalf("Get after Set should return the stored value; got %v want %v", got, state)
	}
	if !s.Has("ns1") {
		t.Fatalf("Has should report true after Set")
	}
	s.Delete("ns1")
	if got := s.Get("ns1"); got != nil {
		t.Fatalf("Get after Delete should return nil; got %v", got)
	}
	if s.Has("ns1") {
		t.Fatalf("Has should report false after Delete")
	}
}

func TestNSAppliedGWState_SetNilClearsEntry(t *testing.T) {
	s := newNSAppliedGWState()
	s.Set("ns1", gwState(map[string]bool{"10.0.0.1": true}))
	s.Set("ns1", nil)
	if s.Has("ns1") {
		t.Fatalf("Set(nil) should clear the entry")
	}
}

func TestNSAppliedGWState_HasDistinguishesNeverSeenFromEmpty(t *testing.T) {
	s := newNSAppliedGWState()
	if s.Has("ns1") {
		t.Fatalf("never-seen ns should not Has")
	}
	s.Set("ns1", newDesiredGWState()) // explicitly empty
	if !s.Has("ns1") {
		t.Fatalf("explicitly-empty ns should Has")
	}
}

func TestNSAppliedGWState_Namespaces(t *testing.T) {
	s := newNSAppliedGWState()
	if got := s.Namespaces(); got != nil {
		t.Fatalf("empty cache should return nil; got %v", got)
	}
	s.Set("ns-c", newDesiredGWState())
	s.Set("ns-a", newDesiredGWState())
	s.Set("ns-b", newDesiredGWState())
	want := []string{"ns-a", "ns-b", "ns-c"}
	if got := s.Namespaces(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Namespaces should be sorted; got %v want %v", got, want)
	}
}

// recordedAdd captures one addRoutesForNamespace call.
type recordedAdd struct {
	ns  string
	ips []string
	bfd bool
}

// recordedDelete captures one deleteRoutesForNamespace call.
type recordedDelete struct {
	ns  string
	ips []string
}

// recordedReplace captures one applyBFDReplaceAtomicallyForNamespace call.
type recordedReplace struct {
	ns         string
	replaceIPs []gwIPBFD
}

// fakeGWRouteProgrammer records calls and returns optional errors. Used
// to verify applyGWStateDelta's call sequence and grouping. Add IPs in
// the recorded calls are sorted to keep test assertions deterministic.
type fakeGWRouteProgrammer struct {
	adds        []recordedAdd
	deletes     []recordedDelete
	replaces    []recordedReplace
	addErr      error
	deleteErr   error
	replaceErr  error
	failOnAddIP string // if non-empty, return addErr only when this IP is in the call
}

func (f *fakeGWRouteProgrammer) addRoutesForNamespace(ns string, info gatewayInfo) error {
	ips := info.gws.UnsortedList()
	sort.Strings(ips)
	if f.failOnAddIP != "" && info.gws.Has(f.failOnAddIP) && f.addErr != nil {
		return f.addErr
	}
	if f.addErr != nil && f.failOnAddIP == "" {
		return f.addErr
	}
	f.adds = append(f.adds, recordedAdd{ns: ns, ips: ips, bfd: info.bfdEnabled})
	return nil
}

func (f *fakeGWRouteProgrammer) deleteRoutesForNamespace(ns string, matchGWs sets.Set[string]) error {
	ips := matchGWs.UnsortedList()
	sort.Strings(ips)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, recordedDelete{ns: ns, ips: ips})
	return nil
}

func (f *fakeGWRouteProgrammer) applyBFDReplaceAtomicallyForNamespace(ns string, replaceIPs []gwIPBFD) error {
	if f.replaceErr != nil {
		return f.replaceErr
	}
	// Copy for deterministic recording.
	dup := make([]gwIPBFD, len(replaceIPs))
	copy(dup, replaceIPs)
	sort.Slice(dup, func(i, j int) bool { return dup[i].ip < dup[j].ip })
	f.replaces = append(f.replaces, recordedReplace{ns: ns, replaceIPs: dup})
	return nil
}

func TestApplyGWStateDelta_Empty(t *testing.T) {
	f := &fakeGWRouteProgrammer{}
	if err := applyGWStateDelta("ns1", gwStateDelta{}, f); err != nil {
		t.Fatalf("empty delta should not error: %v", err)
	}
	if len(f.adds) != 0 || len(f.deletes) != 0 {
		t.Fatalf("empty delta should produce no calls; got adds=%v deletes=%v", f.adds, f.deletes)
	}
}

func TestApplyGWStateDelta_PureAdd_GroupsByBFD(t *testing.T) {
	delta := gwStateDelta{
		add: []gwIPBFD{
			{"10.0.0.1", true},
			{"10.0.0.2", false},
			{"10.0.0.3", true},
		},
	}
	f := &fakeGWRouteProgrammer{}
	if err := applyGWStateDelta("ns1", delta, f); err != nil {
		t.Fatalf("apply should not error: %v", err)
	}
	if len(f.deletes) != 0 {
		t.Fatalf("pure-add should not delete anything; got %v", f.deletes)
	}
	if len(f.adds) != 2 {
		t.Fatalf("expected 2 add calls (one per BFD group); got %d: %v", len(f.adds), f.adds)
	}
	// Iteration order: false first, true second (deterministic by code).
	if !reflect.DeepEqual(f.adds[0], recordedAdd{ns: "ns1", ips: []string{"10.0.0.2"}, bfd: false}) {
		t.Fatalf("first add wrong: %+v", f.adds[0])
	}
	if !reflect.DeepEqual(f.adds[1], recordedAdd{ns: "ns1", ips: []string{"10.0.0.1", "10.0.0.3"}, bfd: true}) {
		t.Fatalf("second add wrong: %+v", f.adds[1])
	}
}

func TestApplyGWStateDelta_PureRemove(t *testing.T) {
	delta := gwStateDelta{remove: []string{"10.0.0.1", "10.0.0.2"}}
	f := &fakeGWRouteProgrammer{}
	if err := applyGWStateDelta("ns1", delta, f); err != nil {
		t.Fatalf("apply should not error: %v", err)
	}
	if len(f.adds) != 0 {
		t.Fatalf("pure-remove should not add anything; got %v", f.adds)
	}
	if len(f.deletes) != 1 {
		t.Fatalf("expected single delete pass; got %d: %v", len(f.deletes), f.deletes)
	}
	want := recordedDelete{ns: "ns1", ips: []string{"10.0.0.1", "10.0.0.2"}}
	if !reflect.DeepEqual(f.deletes[0], want) {
		t.Fatalf("delete wrong: got %+v want %+v", f.deletes[0], want)
	}
}

func TestApplyGWStateDelta_BFDReplace_RoutesThroughAtomicPrimitive(t *testing.T) {
	delta := gwStateDelta{
		replace: []gwIPBFD{{"10.0.0.1", true}},
	}
	f := &fakeGWRouteProgrammer{}
	if err := applyGWStateDelta("ns1", delta, f); err != nil {
		t.Fatalf("apply should not error: %v", err)
	}
	// BFD-replace IPs go through the per-pod atomic primitive, not
	// through the delete+add pair. Per-pod atomicity is the
	// correctness guarantee — no pod briefly loses its route to a
	// replace-target IP.
	if len(f.deletes) != 0 {
		t.Fatalf("BFD-replace should not call delete; got %v", f.deletes)
	}
	if len(f.adds) != 0 {
		t.Fatalf("BFD-replace should not call add; got %v", f.adds)
	}
	if len(f.replaces) != 1 ||
		!reflect.DeepEqual(f.replaces[0], recordedReplace{ns: "ns1", replaceIPs: []gwIPBFD{{"10.0.0.1", true}}}) {
		t.Fatalf("expected single atomic-replace call for the replace IP; got %v", f.replaces)
	}
}

func TestApplyGWStateDelta_Mixed_RemoveReplaceAdd(t *testing.T) {
	delta := gwStateDelta{
		add:     []gwIPBFD{{"10.0.0.4", false}},
		remove:  []string{"10.0.0.3"},
		replace: []gwIPBFD{{"10.0.0.2", true}},
	}
	f := &fakeGWRouteProgrammer{}
	if err := applyGWStateDelta("ns1", delta, f); err != nil {
		t.Fatalf("apply should not error: %v", err)
	}
	// Pure removes: one delete pass with just remove IPs (no replace).
	if len(f.deletes) != 1 || !reflect.DeepEqual(f.deletes[0].ips, []string{"10.0.0.3"}) {
		t.Fatalf("expected single delete pass with remove IPs only; got %v", f.deletes)
	}
	// BFD replaces: one atomic-replace call.
	if len(f.replaces) != 1 ||
		!reflect.DeepEqual(f.replaces[0], recordedReplace{ns: "ns1", replaceIPs: []gwIPBFD{{"10.0.0.2", true}}}) {
		t.Fatalf("expected single atomic-replace call; got %v", f.replaces)
	}
	// Pure adds: one add pass per BFD group (only false here — replace
	// IP doesn't land in the add pass anymore).
	if len(f.adds) != 1 || !reflect.DeepEqual(f.adds[0], recordedAdd{ns: "ns1", ips: []string{"10.0.0.4"}, bfd: false}) {
		t.Fatalf("expected single add pass for pure-add IP; got %v", f.adds)
	}
}

func TestApplyGWStateDelta_DeleteFailureSkipsAdds(t *testing.T) {
	delta := gwStateDelta{
		add:    []gwIPBFD{{"10.0.0.1", false}},
		remove: []string{"10.0.0.9"},
	}
	wantErr := errors.New("delete kaboom")
	f := &fakeGWRouteProgrammer{deleteErr: wantErr}
	err := applyGWStateDelta("ns1", delta, f)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected delete error to surface; got %v", err)
	}
	if len(f.adds) != 0 {
		t.Fatalf("delete failure should skip add pass; got %v", f.adds)
	}
}

func TestApplyGWStateDelta_AddFailurePropagates(t *testing.T) {
	delta := gwStateDelta{
		add: []gwIPBFD{
			{"10.0.0.1", false},
			{"10.0.0.2", true},
		},
	}
	wantErr := errors.New("add kaboom")
	f := &fakeGWRouteProgrammer{addErr: wantErr}
	err := applyGWStateDelta("ns1", delta, f)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected add error to surface; got %v", err)
	}
}

func TestApplyGWStateDelta_ReplaceFailurePropagates(t *testing.T) {
	delta := gwStateDelta{
		replace: []gwIPBFD{{"10.0.0.1", true}},
	}
	wantErr := errors.New("replace kaboom")
	f := &fakeGWRouteProgrammer{replaceErr: wantErr}
	err := applyGWStateDelta("ns1", delta, f)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected replace error to surface; got %v", err)
	}
	if len(f.adds) != 0 {
		t.Fatalf("replace failure should skip add pass; got %v", f.adds)
	}
}

func TestApplyGWStateDelta_RemoveBeforeReplaceBeforeAdd(t *testing.T) {
	// Ensure the call order is remove → replace → add. The order is
	// load-bearing: replace IPs are independently atomic per pod, so
	// they don't need a global ordering against remove or add, but
	// keeping the sequence deterministic helps operator-visible logs
	// and avoids surprises if a future change introduces ordering
	// dependencies.
	delta := gwStateDelta{
		add:     []gwIPBFD{{"10.0.0.5", true}},
		remove:  []string{"10.0.0.3"},
		replace: []gwIPBFD{{"10.0.0.7", false}},
	}
	var calls []orderRecorderCall
	wrap := &orderRecorderProgrammer{inner: &fakeGWRouteProgrammer{}, calls: &calls}
	if err := applyGWStateDelta("ns1", delta, wrap); err != nil {
		t.Fatalf("apply should not error: %v", err)
	}
	want := []orderRecorderCall{
		{op: "delete", ns: "ns1"},
		{op: "replace", ns: "ns1"},
		{op: "add", ns: "ns1"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("call order: got %v want %v", calls, want)
	}
}

type orderRecorderCall struct {
	op string
	ns string
}

type orderRecorderProgrammer struct {
	inner *fakeGWRouteProgrammer
	calls *[]orderRecorderCall
}

func (o *orderRecorderProgrammer) addRoutesForNamespace(ns string, info gatewayInfo) error {
	*o.calls = append(*o.calls, orderRecorderCall{op: "add", ns: ns})
	return o.inner.addRoutesForNamespace(ns, info)
}

func (o *orderRecorderProgrammer) deleteRoutesForNamespace(ns string, matchGWs sets.Set[string]) error {
	*o.calls = append(*o.calls, orderRecorderCall{op: "delete", ns: ns})
	return o.inner.deleteRoutesForNamespace(ns, matchGWs)
}

func (o *orderRecorderProgrammer) applyBFDReplaceAtomicallyForNamespace(ns string, replaceIPs []gwIPBFD) error {
	*o.calls = append(*o.calls, orderRecorderCall{op: "replace", ns: ns})
	return o.inner.applyBFDReplaceAtomicallyForNamespace(ns, replaceIPs)
}

func TestApplyGWStateDelta_PartialAddFailure_SecondPassNotRun(t *testing.T) {
	// Verify that if the first BFD group's add fails, the second
	// group is not attempted — the namespace is left for the next
	// reconcile to retry.
	delta := gwStateDelta{
		add: []gwIPBFD{
			{"10.0.0.1", false},
			{"10.0.0.2", true},
		},
	}
	wantErr := errors.New("add kaboom")
	f := &fakeGWRouteProgrammer{addErr: wantErr, failOnAddIP: "10.0.0.1"}
	err := applyGWStateDelta("ns1", delta, f)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected add error; got %v", err)
	}
	// false group fired (and failed); true group did not.
	if len(f.adds) != 0 {
		t.Fatalf("failed first-group add should leave 0 successful records; got %v", f.adds)
	}
}

func TestNSAppliedGWState_ConcurrentAccess(t *testing.T) {
	// Sanity check: concurrent Get/Set/Delete should not race under
	// the RWMutex protection. Run with `-race`.
	s := newNSAppliedGWState()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Set("ns1", gwState(map[string]bool{"10.0.0.1": true}))
				_ = s.Get("ns1")
				s.Delete("ns1")
			}
		}()
	}
	wg.Wait()
}
