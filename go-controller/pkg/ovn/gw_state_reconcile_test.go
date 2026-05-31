// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/nbdb"
	apbroutecontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/ovn/controller/apbroute"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func nsWithAnnotations(name string, anno map[string]string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: anno},
	}
}

func sortedGWList(info gatewayInfo) []string {
	if info.gws == nil {
		return nil
	}
	out := info.gws.UnsortedList()
	sort.Strings(out)
	return out
}

func TestParseAnnotationGWs_NoAnnotation(t *testing.T) {
	got := parseAnnotationGWs(nsWithAnnotations("ns", nil))
	if got.gws != nil {
		t.Fatalf("no annotation should yield nil gws; got %v", got.gws)
	}
	if got.bfdEnabled {
		t.Fatalf("no annotation should yield bfdEnabled=false; got true")
	}
}

func TestParseAnnotationGWs_EmptyAnnotation(t *testing.T) {
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.RoutingExternalGWsAnnotation: "",
	}))
	if got.gws != nil {
		t.Fatalf("empty annotation should yield nil gws; got %v", got.gws)
	}
}

func TestParseAnnotationGWs_SingleGW(t *testing.T) {
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.RoutingExternalGWsAnnotation: "10.0.0.1",
	}))
	want := []string{"10.0.0.1"}
	if !reflect.DeepEqual(sortedGWList(got), want) {
		t.Fatalf("single GW: got %v want %v", sortedGWList(got), want)
	}
	if got.bfdEnabled {
		t.Fatalf("no BFD annotation should yield bfdEnabled=false")
	}
}

func TestParseAnnotationGWs_MultipleGWs(t *testing.T) {
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.RoutingExternalGWsAnnotation: "10.0.0.1,10.0.0.2,10.0.0.3",
	}))
	want := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	if !reflect.DeepEqual(sortedGWList(got), want) {
		t.Fatalf("multi GW: got %v want %v", sortedGWList(got), want)
	}
}

func TestParseAnnotationGWs_BFDEnabled(t *testing.T) {
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.RoutingExternalGWsAnnotation: "10.0.0.1",
		util.BfdAnnotation:                "",
	}))
	if !got.bfdEnabled {
		t.Fatalf("BFD annotation present should yield bfdEnabled=true; got false")
	}
}

func TestParseAnnotationGWs_BFDOnlyNoGWAnnotation(t *testing.T) {
	// BFD annotation alone (no routing-external-gws) yields the zero
	// value: no gws, no BFD. Matches what configureNamespace observed
	// (the routing-external-gws read is the gate).
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.BfdAnnotation: "",
	}))
	if got.gws != nil {
		t.Fatalf("BFD-only should yield nil gws; got %v", got.gws)
	}
	if got.bfdEnabled {
		t.Fatalf("BFD-only without routing annotation should yield bfdEnabled=false")
	}
}

func TestParseAnnotationGWs_MalformedReturnsZero(t *testing.T) {
	// A malformed annotation does not panic and returns the zero
	// value; the apply primitive's caller (configureNamespace /
	// updateNamespace) is responsible for surfacing parse errors via
	// its own ParseRoutingExternalGWAnnotation call.
	got := parseAnnotationGWs(nsWithAnnotations("ns", map[string]string{
		util.RoutingExternalGWsAnnotation: "not-an-ip",
	}))
	if got.gws != nil {
		t.Fatalf("malformed annotation should yield nil gws; got %v", got.gws)
	}
	if got.bfdEnabled {
		t.Fatalf("malformed annotation should not propagate BFD; got true")
	}
}

func TestGRNameFromOutputPort(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Default ext-switch shape: "rtoe-<gr>".
		{"rtoe-GR_worker-1", "GR_worker-1"},
		// Multi-zone/transit case: "<prefix>rtoe-<gr>".
		{"transit-rtoe-GR_worker-2", "GR_worker-2"},
		// LastIndex semantics: if "rtoe-" appears in the GR name itself
		// (highly unlikely but defensive), the trailing instance wins.
		{"rtoe-GR_rtoe-2", "2"},
		{"no-prefix-here", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := grNameFromOutputPort(tc.in); got != tc.want {
			t.Errorf("grNameFromOutputPort(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSeedGWStateFromRoutes_PopulatesBothCaches(t *testing.T) {
	// Restart scenario: NBDB has stale routes for two pods in ns-target
	// (one with BFD, one without) and one orphan route for a pod IP no
	// longer in the informer. Bootstrap must seed both the per-namespace
	// applied snapshot AND the per-pod gateway-route cache so that the
	// next reconcile can diff + execute the delete leg.
	policy := nbdb.LogicalRouterStaticRoutePolicySrcIP
	bfd := "bfd-uuid-1"
	routes := []*nbdb.LogicalRouterStaticRoute{
		{
			IPPrefix:   "10.0.0.5/32",
			Nexthop:    "192.168.1.1",
			Policy:     &policy,
			OutputPort: ptr.To("rtoe-GR_worker-a"),
			Options:    map[string]string{"ecmp_symmetric_reply": "true"},
		},
		{
			IPPrefix:   "10.0.0.6/32",
			Nexthop:    "192.168.1.2",
			Policy:     &policy,
			OutputPort: ptr.To("rtoe-GR_worker-a"),
			Options:    map[string]string{"ecmp_symmetric_reply": "true"},
			BFD:        &bfd,
		},
		{
			// Orphan: pod IP not in the informer map; must be skipped.
			IPPrefix:   "10.0.99.99/32",
			Nexthop:    "192.168.1.3",
			Policy:     &policy,
			OutputPort: ptr.To("rtoe-GR_worker-a"),
			Options:    map[string]string{"ecmp_symmetric_reply": "true"},
		},
	}
	podIPToPod := map[string]ktypes.NamespacedName{
		"10.0.0.5": {Namespace: "ns-target", Name: "pod-a"},
		"10.0.0.6": {Namespace: "ns-target", Name: "pod-b"},
	}

	applied := newNSAppliedGWState()
	routeCache := apbroutecontroller.NewExternalGatewayRouteInfoCache()

	seedGWStateFromRoutes(routes, podIPToPod, applied, routeCache)

	// nsAppliedGWState: ns-target should have both gateway IPs, with
	// BFD reflecting per-route flag.
	got := applied.Get("ns-target")
	if got == nil {
		t.Fatal("expected ns-target to be seeded in nsAppliedGWState")
	}
	entries := got.entries()
	bfd1, ok1 := entries["192.168.1.1"]
	bfd2, ok2 := entries["192.168.1.2"]
	if !ok1 || !ok2 {
		t.Fatalf("ns-target applied state missing gateway IPs; got entries: %v", entries)
	}
	if bfd1 {
		t.Errorf("expected BFD=false for 192.168.1.1, got true")
	}
	if !bfd2 {
		t.Errorf("expected BFD=true for 192.168.1.2 (it had a non-nil BFD uuid), got false")
	}

	// externalGatewayRouteInfo: each pod must have an entry with its
	// route. Without this cache, deleteGWRoutesForNamespace silently
	// no-ops on restart — the regression this test fronts.
	verifyPodEntry := func(t *testing.T, key ktypes.NamespacedName, podIP, gw, gr string) {
		t.Helper()
		err := routeCache.Cleanup(key, func(routeInfo *apbroutecontroller.RouteInfo) error {
			routes, ok := routeInfo.PodExternalRoutes[podIP]
			if !ok {
				t.Errorf("expected route entry for %s podIP=%s; PodExternalRoutes=%v", key, podIP, routeInfo.PodExternalRoutes)
				return nil
			}
			if got := routes[gw]; got != gr {
				t.Errorf("expected GR=%q for %s/%s, got %q", gr, key, gw, got)
			}
			return nil
		})
		if err != nil {
			t.Errorf("routeCache.Cleanup(%s): %v", key, err)
		}
	}
	verifyPodEntry(t, ktypes.NamespacedName{Namespace: "ns-target", Name: "pod-a"}, "10.0.0.5", "192.168.1.1", "GR_worker-a")
	verifyPodEntry(t, ktypes.NamespacedName{Namespace: "ns-target", Name: "pod-b"}, "10.0.0.6", "192.168.1.2", "GR_worker-a")
}

func TestSeedGWStateFromRoutes_SkipsRouteWithoutOutputPort(t *testing.T) {
	// Defensive: bootstrap predicate filters out routes without
	// OutputPort, but if a future predicate change loosens this,
	// seedGWStateFromRoutes must not panic.
	policy := nbdb.LogicalRouterStaticRoutePolicySrcIP
	routes := []*nbdb.LogicalRouterStaticRoute{
		{
			IPPrefix: "10.0.0.5/32",
			Nexthop:  "192.168.1.1",
			Policy:   &policy,
			Options:  map[string]string{"ecmp_symmetric_reply": "true"},
		},
	}
	applied := newNSAppliedGWState()
	routeCache := apbroutecontroller.NewExternalGatewayRouteInfoCache()
	seedGWStateFromRoutes(routes,
		map[string]ktypes.NamespacedName{"10.0.0.5": {Namespace: "ns-x", Name: "pod-x"}},
		applied, routeCache)
	// nsAppliedGWState is still seeded (the applied snapshot doesn't
	// need the GR name); the route cache is just skipped.
	if applied.Get("ns-x") == nil {
		t.Fatal("ns-x must be present in applied state even when OutputPort is missing")
	}
}


func TestRunGWReconcile_RunsSideEffectsOnEmptyDelta(t *testing.T) {
	// Regression: previously reconcileGWStateForNamespace returned
	// early when computeGWStateDelta(applied, desired) was empty,
	// skipping applyGWStateSideEffects. On bootstrap, NBDB routes can
	// match desired (so delta is empty) while the external-gw-pod-ips
	// annotation is stale — the side-effects path is what re-publishes
	// the annotation, so skipping it left the stale value indefinitely.
	// Codex Medium finding. Fix: always run side effects.
	progr := &fakeGWRouteProgrammer{}
	snapshot := newNSAppliedGWState()
	desired := newDesiredGWState()
	desired.addGW("10.0.0.1", false)
	applied := newDesiredGWState()
	applied.addGW("10.0.0.1", false) // identical → empty delta

	var sideEffectCalls int
	var sideEffectDesired *desiredGWState
	sideEffects := func(ns string, d *desiredGWState) error {
		sideEffectCalls++
		sideEffectDesired = d
		return nil
	}

	err := runGWReconcile("ns-target", applied, desired, progr, sideEffects, snapshot)
	if err != nil {
		t.Fatalf("runGWReconcile: %v", err)
	}

	// Programmer must NOT be called (delta empty).
	if len(progr.adds) != 0 || len(progr.deletes) != 0 || len(progr.replaces) != 0 {
		t.Fatalf("delta was empty; programmer must not be called (got add=%d del=%d replace=%d)",
			len(progr.adds), len(progr.deletes), len(progr.replaces))
	}
	// Side effects MUST run.
	if sideEffectCalls != 1 {
		t.Fatalf("side effects must run on empty delta; got %d calls", sideEffectCalls)
	}
	if sideEffectDesired != desired {
		t.Errorf("side effects received wrong desired pointer")
	}
	// Snapshot must be updated (desired non-empty → Set).
	if snapshot.Get("ns-target") == nil {
		t.Errorf("snapshot must be Set on non-empty desired even with empty delta")
	}
}

func TestRunGWReconcile_RunsSideEffectsOnNonEmptyDelta(t *testing.T) {
	// Companion to the empty-delta case: when delta is non-empty, the
	// programmer fires AND side effects still run. Verifies side
	// effects aren't gated on either branch.
	progr := &fakeGWRouteProgrammer{}
	snapshot := newNSAppliedGWState()
	desired := newDesiredGWState()
	desired.addGW("10.0.0.1", false)
	desired.addGW("10.0.0.2", false)
	applied := newDesiredGWState()
	applied.addGW("10.0.0.1", false) // missing 10.0.0.2 → add delta

	var sideEffectCalls int
	sideEffects := func(ns string, d *desiredGWState) error {
		sideEffectCalls++
		return nil
	}

	err := runGWReconcile("ns-target", applied, desired, progr, sideEffects, snapshot)
	if err != nil {
		t.Fatalf("runGWReconcile: %v", err)
	}
	if len(progr.adds) != 1 {
		t.Fatalf("expected one add pass for the new IP; got %d", len(progr.adds))
	}
	if sideEffectCalls != 1 {
		t.Fatalf("side effects must run after delta apply; got %d calls", sideEffectCalls)
	}
}

func TestRunGWReconcile_EmptyDesiredDeletesSnapshot(t *testing.T) {
	// When desired is empty (e.g., the only gateway pod just left),
	// the snapshot entry must be Deleted, not Set with an empty state.
	progr := &fakeGWRouteProgrammer{}
	snapshot := newNSAppliedGWState()
	applied := newDesiredGWState()
	applied.addGW("10.0.0.1", false)
	snapshot.Set("ns-target", applied) // pre-existing
	desired := newDesiredGWState()     // empty

	sideEffectsRan := false
	sideEffects := func(ns string, d *desiredGWState) error {
		sideEffectsRan = true
		return nil
	}

	err := runGWReconcile("ns-target", applied, desired, progr, sideEffects, snapshot)
	if err != nil {
		t.Fatalf("runGWReconcile: %v", err)
	}
	if !sideEffectsRan {
		t.Error("side effects must run on empty-desired transition")
	}
	if snapshot.Get("ns-target") != nil {
		t.Error("snapshot entry must be Deleted when desired is empty")
	}
}

func TestRunGWReconcile_DeltaErrorShortCircuits(t *testing.T) {
	// Verify side effects DON'T run when applyGWStateDelta fails. The
	// fix decoupled side effects from delta presence, but they're
	// still gated on delta SUCCESS — otherwise we'd publish an
	// annotation that doesn't reflect actual NBDB state.
	progr := &fakeGWRouteProgrammer{
		addErr: errors.New("nbdb down"),
	}
	snapshot := newNSAppliedGWState()
	desired := newDesiredGWState()
	desired.addGW("10.0.0.1", false)
	applied := newDesiredGWState() // empty → add delta

	sideEffectsRan := false
	sideEffects := func(ns string, d *desiredGWState) error {
		sideEffectsRan = true
		return nil
	}

	err := runGWReconcile("ns-target", applied, desired, progr, sideEffects, snapshot)
	if err == nil {
		t.Fatal("expected runGWReconcile to surface programmer error")
	}
	if sideEffectsRan {
		t.Error("side effects must NOT run when delta apply errors")
	}
	if snapshot.Get("ns-target") != nil {
		t.Error("snapshot must NOT be updated on delta error")
	}
}

func TestRunGWReconcile_SideEffectErrorRetriesWithoutAdvancingSnapshot(t *testing.T) {
	// A retryable side-effect error (e.g. the multi-zone APB
	// gateway-IP lookup failing before any conntrack flush) must
	// propagate so the workqueue retries, and must NOT advance the
	// applied snapshot — otherwise the side effect is skipped
	// permanently with no retry. Best-effort failures (annotation
	// patch, conntrack flush) are swallowed inside
	// applyGWStateSideEffects and never reach runGWReconcile, so they
	// don't trigger this path.
	progr := &fakeGWRouteProgrammer{}
	snapshot := newNSAppliedGWState()
	desired := newDesiredGWState()
	desired.addGW("10.0.0.1", false)
	applied := newDesiredGWState()
	applied.addGW("10.0.0.1", false) // identical → empty delta, side effects still run

	wantErr := errors.New("APB gateway-IP lookup failed")
	sideEffects := func(ns string, d *desiredGWState) error { return wantErr }

	err := runGWReconcile("ns-target", applied, desired, progr, sideEffects, snapshot)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped side-effect error, got %v", err)
	}
	if snapshot.Get("ns-target") != nil {
		t.Error("snapshot must NOT advance when side effects return a retryable error")
	}
}
