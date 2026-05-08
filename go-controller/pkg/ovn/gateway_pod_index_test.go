// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"reflect"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// hostNetReadyGW returns a host-network gateway pod with the given
// targets, IPs, BFD flag, and readiness state.
func hostNetReadyGW(name, ns, targets string, ips []string, bfd bool, ready bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{HostNetwork: true},
	}
	pod.Annotations = map[string]string{
		util.RoutingNamespaceAnnotation: targets,
	}
	if bfd {
		pod.Annotations[util.BfdAnnotation] = ""
	}
	for _, ip := range ips {
		pod.Status.PodIPs = append(pod.Status.PodIPs, corev1.PodIP{IP: ip})
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
	}
	return pod
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestGatewayPodIndex_HasSynced(t *testing.T) {
	idx := newGatewayPodIndex()
	if idx.HasSynced() {
		t.Fatal("HasSynced should be false on fresh index")
	}
	idx.BootstrapFromPodList(nil)
	if !idx.HasSynced() {
		t.Fatal("HasSynced should be true after BootstrapFromPodList")
	}
}

func TestGatewayPodIndex_NotACandidate(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "p1"},
		// No routing-namespaces annotation → not a gateway pod.
	}
	out := idx.Update(pod)
	if out.Len() != 0 {
		t.Fatalf("non-candidate pod should yield empty fan-out, got %v", sortedList(out))
	}
	if _, ok := idx.PayloadFor(makePodGWKey(pod)); ok {
		t.Fatalf("non-candidate pod must not be stored")
	}
}

func TestGatewayPodIndex_AddNewGatewayPod(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := hostNetReadyGW("p1", "gw-ns", "ns-a,ns-b", []string{"10.0.0.1", "10.0.0.2"}, false, true)
	out := idx.Update(pod)
	if !out.Equal(sets.New("ns-a", "ns-b")) {
		t.Fatalf("affected ns set wrong: got %v want [ns-a, ns-b]", sortedList(out))
	}

	gwsA := idx.GatewaysForNamespace("ns-a")
	if !reflect.DeepEqual(sortedKeys(gwsA), []string{"10.0.0.1", "10.0.0.2"}) {
		t.Fatalf("ns-a gws wrong: got %v", sortedKeys(gwsA))
	}
	if gwsA["10.0.0.1"] || gwsA["10.0.0.2"] {
		t.Fatalf("BFD must be false: got %v", gwsA)
	}

	pods := idx.PodsForNamespace("ns-a")
	if !reflect.DeepEqual(pods, []string{"gw-ns_p1"}) {
		t.Fatalf("PodsForNamespace wrong: got %v", pods)
	}
}

func TestGatewayPodIndex_BFDFlag(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, true, true)
	idx.Update(pod)
	gws := idx.GatewaysForNamespace("ns-a")
	if !gws["10.0.0.1"] {
		t.Fatalf("BFD must be true on this gw: got %v", gws)
	}
}

func TestGatewayPodIndex_ORBFDCollision(t *testing.T) {
	idx := newGatewayPodIndex()
	// Two distinct gateway pods, same target ns, both contributing the
	// same IP — but one with BFD and one without. OR-on-collision means
	// the merged BFD must be true.
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true))
	idx.Update(hostNetReadyGW("p2", "gw-ns", "ns-a", []string{"10.0.0.1"}, true, true))

	gws := idx.GatewaysForNamespace("ns-a")
	if !gws["10.0.0.1"] {
		t.Fatalf("merged BFD should be true via OR-on-collision: got %v", gws)
	}
}

func TestGatewayPodIndex_TargetSetChange(t *testing.T) {
	idx := newGatewayPodIndex()
	pod1 := hostNetReadyGW("p1", "gw-ns", "ns-a,ns-b", []string{"10.0.0.1"}, false, true)
	idx.Update(pod1)

	// Now drop ns-a, add ns-c. Affected fan-out is {ns-a, ns-b, ns-c}.
	pod2 := hostNetReadyGW("p1", "gw-ns", "ns-b,ns-c", []string{"10.0.0.1"}, false, true)
	out := idx.Update(pod2)
	want := sets.New("ns-a", "ns-b", "ns-c")
	if !out.Equal(want) {
		t.Fatalf("affected ns set wrong on target-set change: got %v want %v", sortedList(out), sortedList(want))
	}

	// ns-a no longer has gateways from p1.
	if len(idx.GatewaysForNamespace("ns-a")) != 0 {
		t.Fatalf("ns-a should have no gateways after drop, got %v", idx.GatewaysForNamespace("ns-a"))
	}
	// ns-b and ns-c each have the IP (presence, not BFD value).
	if _, ok := idx.GatewaysForNamespace("ns-b")["10.0.0.1"]; !ok {
		t.Fatalf("ns-b should still have the IP")
	}
	if _, ok := idx.GatewaysForNamespace("ns-c")["10.0.0.1"]; !ok {
		t.Fatalf("ns-c should now have the IP")
	}
}

func TestGatewayPodIndex_BFDToggleSameTargets(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true)
	idx.Update(pod)

	// Toggle BFD; target-set unchanged. Affected fan-out must STILL
	// include ns-a so namespace reconcile can drive route reconvergence.
	pod2 := hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, true, true)
	out := idx.Update(pod2)
	if !out.Equal(sets.New("ns-a")) {
		t.Fatalf("BFD toggle must still fan out to target ns: got %v", sortedList(out))
	}
	if !idx.GatewaysForNamespace("ns-a")["10.0.0.1"] {
		t.Fatalf("BFD must be true after toggle: got %v", idx.GatewaysForNamespace("ns-a"))
	}
}

func TestGatewayPodIndex_HasActiveGWPods(t *testing.T) {
	idx := newGatewayPodIndex()

	if idx.HasActiveGWPods("ns-a") {
		t.Fatal("empty index must not report active gateway pods")
	}

	// Ready pod → active, HasActiveGWPods must return true.
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true))
	if !idx.HasActiveGWPods("ns-a") {
		t.Fatal("ready pod must count as an active gateway pod")
	}

	// Pod becomes not-ready: payload stays in the index (so
	// PodsForNamespace still returns it), but active==false.
	// HasActiveGWPods must return false — this is the load-bearing
	// difference vs. len(PodsForNamespace) > 0 that drives the
	// SNAT-restore fan-out gate in updateNamespace.
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, false))
	if len(idx.PodsForNamespace("ns-a")) == 0 {
		t.Fatal("test premise: inactive payload must remain in PodsForNamespace view")
	}
	if idx.HasActiveGWPods("ns-a") {
		t.Fatal("not-ready pod must not count as an active gateway pod")
	}

	// Empty-namespace lookup is safe.
	if idx.HasActiveGWPods("ns-unknown") {
		t.Fatal("unknown namespace must report no active gateway pods")
	}
}

func TestGatewayPodIndex_NotReadyDropsActive(t *testing.T) {
	idx := newGatewayPodIndex()
	// Ready pod first.
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true))
	if len(idx.GatewaysForNamespace("ns-a")) == 0 {
		t.Fatalf("expected active gateways for ns-a")
	}

	// Pod becomes not-ready: payload still in index, but inactive — so
	// namespace reconcile sees an empty desired-state for ns-a.
	notReady := hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, false)
	out := idx.Update(notReady)
	if !out.Equal(sets.New("ns-a")) {
		t.Fatalf("ready→not-ready must fan out to target ns: got %v", sortedList(out))
	}
	if len(idx.GatewaysForNamespace("ns-a")) != 0 {
		t.Fatalf("not-ready pod must not contribute to desired-state: got %v", idx.GatewaysForNamespace("ns-a"))
	}
}

func TestGatewayPodIndex_DeleteCandidateAnnotationGone(t *testing.T) {
	idx := newGatewayPodIndex()
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a,ns-b", []string{"10.0.0.1"}, false, true))

	// Annotation removed entirely → not a candidate. Forward+reverse
	// entries must be dropped; affected ns set is the prior targets.
	notCandidate := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "p1"},
	}
	out := idx.Update(notCandidate)
	if !out.Equal(sets.New("ns-a", "ns-b")) {
		t.Fatalf("annotation-gone must fan out to prior targets: got %v", sortedList(out))
	}
	if _, ok := idx.PayloadFor(makePodGWKey(notCandidate)); ok {
		t.Fatalf("annotation-gone must drop the index entry")
	}
	if len(idx.PodsForNamespace("ns-a")) != 0 {
		t.Fatalf("ns-a reverse map must be empty after annotation-gone")
	}
}

func TestGatewayPodIndex_DeletePod(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := hostNetReadyGW("p1", "gw-ns", "ns-a,ns-b", []string{"10.0.0.1"}, false, true)
	idx.Update(pod)

	out := idx.Delete(makePodGWKey(pod))
	if !out.Equal(sets.New("ns-a", "ns-b")) {
		t.Fatalf("Delete must return prior targets: got %v", sortedList(out))
	}
	if _, ok := idx.PayloadFor(makePodGWKey(pod)); ok {
		t.Fatalf("Delete must drop the index entry")
	}
}

func TestGatewayPodIndex_DeleteUnknown(t *testing.T) {
	idx := newGatewayPodIndex()
	out := idx.Delete("does-not-exist")
	if out.Len() != 0 {
		t.Fatalf("Delete of unknown pod should return empty set, got %v", sortedList(out))
	}
}

func TestGatewayPodIndex_BootstrapFromPodList(t *testing.T) {
	idx := newGatewayPodIndex()
	pods := []*corev1.Pod{
		hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true),
		hostNetReadyGW("p2", "gw-ns", "ns-a,ns-b", []string{"10.0.0.2"}, true, true),
		// non-candidate
		{ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: "irrelevant"}},
		// candidate but not ready
		hostNetReadyGW("p3", "gw-ns", "ns-c", []string{"10.0.0.3"}, false, false),
	}
	idx.BootstrapFromPodList(pods)

	if !idx.HasSynced() {
		t.Fatal("HasSynced must flip after bootstrap")
	}

	// ns-a: p1 contributes 10.0.0.1 (no BFD), p2 contributes 10.0.0.2 (BFD).
	gwsA := idx.GatewaysForNamespace("ns-a")
	if !reflect.DeepEqual(sortedKeys(gwsA), []string{"10.0.0.1", "10.0.0.2"}) {
		t.Fatalf("ns-a gws wrong post-bootstrap: got %v", sortedKeys(gwsA))
	}
	if gwsA["10.0.0.1"] || !gwsA["10.0.0.2"] {
		t.Fatalf("ns-a BFD wrong: got %v", gwsA)
	}

	// ns-b: only p2.
	gwsB := idx.GatewaysForNamespace("ns-b")
	if !reflect.DeepEqual(sortedKeys(gwsB), []string{"10.0.0.2"}) || !gwsB["10.0.0.2"] {
		t.Fatalf("ns-b post-bootstrap wrong: got %v", gwsB)
	}

	// ns-c: candidate but not-ready → present in payload but not active.
	if len(idx.GatewaysForNamespace("ns-c")) != 0 {
		t.Fatalf("ns-c should have no active gateways, got %v", idx.GatewaysForNamespace("ns-c"))
	}
}

func TestGatewayPodIndex_BootstrapReplacesState(t *testing.T) {
	idx := newGatewayPodIndex()
	idx.BootstrapFromPodList([]*corev1.Pod{
		hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true),
	})
	idx.BootstrapFromPodList([]*corev1.Pod{
		hostNetReadyGW("p2", "gw-ns", "ns-b", []string{"10.0.0.2"}, false, true),
	})
	if len(idx.GatewaysForNamespace("ns-a")) != 0 {
		t.Fatalf("re-bootstrap should clear ns-a, got %v", idx.GatewaysForNamespace("ns-a"))
	}
	if len(idx.GatewaysForNamespace("ns-b")) == 0 {
		t.Fatalf("re-bootstrap should populate ns-b")
	}
}

func TestGatewayPodIndex_PerPodGatewaysForNamespace(t *testing.T) {
	idx := newGatewayPodIndex()
	idx.Update(hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1", "10.0.0.2"}, false, true))
	idx.Update(hostNetReadyGW("p2", "gw-ns", "ns-a", []string{"10.0.0.3"}, true, true))
	// Not-ready pod must NOT appear (active==false).
	idx.Update(hostNetReadyGW("p3", "gw-ns", "ns-a", []string{"10.0.0.4"}, false, false))

	got := idx.PerPodGatewaysForNamespace("ns-a")
	if len(got) != 2 {
		t.Fatalf("expected 2 active entries; got %d (%+v)", len(got), got)
	}
	p1 := got["gw-ns_p1"]
	if !p1.gws.Equal(sets.New("10.0.0.1", "10.0.0.2")) || p1.bfdEnabled {
		t.Fatalf("p1 entry wrong: gws=%v bfd=%v", sortedList(p1.gws), p1.bfdEnabled)
	}
	p2 := got["gw-ns_p2"]
	if !p2.gws.Equal(sets.New("10.0.0.3")) || !p2.bfdEnabled {
		t.Fatalf("p2 entry wrong: gws=%v bfd=%v", sortedList(p2.gws), p2.bfdEnabled)
	}
	if _, hasP3 := got["gw-ns_p3"]; hasP3 {
		t.Fatalf("not-ready p3 must be excluded; got %+v", got)
	}

	// Mutating the returned map / inner sets must not affect the index.
	got["gw-ns_p1"].gws.Insert("9.9.9.9")
	again := idx.PerPodGatewaysForNamespace("ns-a")
	if again["gw-ns_p1"].gws.Has("9.9.9.9") {
		t.Fatalf("mutation leaked into index")
	}
}

func TestGatewayPodIndex_PayloadCopyIsolation(t *testing.T) {
	idx := newGatewayPodIndex()
	pod := hostNetReadyGW("p1", "gw-ns", "ns-a", []string{"10.0.0.1"}, false, true)
	idx.Update(pod)
	got, ok := idx.PayloadFor(makePodGWKey(pod))
	if !ok {
		t.Fatal("PayloadFor should find the pod")
	}
	got.targetNS.Insert("inserted")
	got.gws.Insert("9.9.9.9")
	// Re-fetch and confirm the index wasn't mutated by caller mutation.
	got2, _ := idx.PayloadFor(makePodGWKey(pod))
	if got2.targetNS.Has("inserted") {
		t.Fatalf("PayloadFor leaked targetNS into cache: %v", sortedList(got2.targetNS))
	}
	if got2.gws.Has("9.9.9.9") {
		t.Fatalf("PayloadFor leaked gws into cache: %v", sortedList(got2.gws))
	}
}
