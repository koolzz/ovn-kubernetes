// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"reflect"
	"sync"
	"testing"
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
