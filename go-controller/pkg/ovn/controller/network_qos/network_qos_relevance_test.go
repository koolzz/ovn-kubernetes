// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package networkqos

import (
	"sync"
	"testing"

	nadlister "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	nqostype "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/networkqos/v1alpha1"
	crdtypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/types"
	ovntest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func newRelevanceTestController(tb testing.TB) (*Controller, cache.Indexer, cache.Indexer, cache.Indexer) {
	tb.Helper()
	c, policies := newEventTestController(tb)
	nads := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	namespaces := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	c.nadLister = nadlister.NewNetworkAttachmentDefinitionLister(nads)
	c.nqosNamespaceLister = corelister.NewNamespaceLister(namespaces)
	nad := ovntest.GenerateNAD("network-a", "network-a", "ns", types.LocalnetTopology, "10.0.0.0/24", types.NetworkRoleSecondary)
	nad.Labels = map[string]string{"network": "a"}
	info, err := util.ParseNADInfo(nad)
	if err != nil {
		tb.Fatal(err)
	}
	c.NetInfo = util.NewMutableNetInfo(info)
	if err := nads.Add(nad); err != nil {
		tb.Fatal(err)
	}
	if err := namespaces.Add(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", ResourceVersion: "1"}}); err != nil {
		tb.Fatal(err)
	}
	return c, policies, nads, namespaces
}

func policyForNAD(label string) *nqostype.NetworkQoS {
	return &nqostype.NetworkQoS{
		ObjectMeta: metav1.ObjectMeta{Name: "qos", Namespace: "ns", ResourceVersion: "1"},
		Spec: nqostype.Spec{NetworkSelectors: crdtypes.NetworkSelectors{{
			NetworkSelectionType: crdtypes.NetworkAttachmentDefinitions,
			NetworkAttachmentDefinitionSelector: &crdtypes.NetworkAttachmentDefinitionSelector{
				NetworkSelector: metav1.LabelSelector{MatchLabels: map[string]string{"network": label}},
			},
		}}},
	}
}

func TestPolicyRelevanceChangesBeforeReconciliation(t *testing.T) {
	c, policies, _, _ := newRelevanceTestController(t)
	policy := policyForNAD("b")
	if err := policies.Add(policy); err != nil {
		t.Fatal(err)
	}
	c.onNQOSAdd(policy)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}
	c.onNQOSPodAdd(pod)
	if c.nqosPodQueue.Len() != 0 {
		t.Fatal("policy for another network enabled Pod events")
	}
	updated := policy.DeepCopy()
	updated.ResourceVersion = "2"
	updated.Spec.NetworkSelectors[0].NetworkAttachmentDefinitionSelector.NetworkSelector.MatchLabels["network"] = "a"
	if err := policies.Update(updated); err != nil {
		t.Fatal(err)
	}
	c.onNQOSUpdate(policy, updated)
	// No policy worker has run. The new desired selector must already allow
	// events, including destination Pods on another node and in another namespace.
	pod.Namespace, pod.Spec.NodeName = "remote-destination", "remote-node"
	c.onNQOSPodAdd(pod)
	if c.nqosPodQueue.Len() != 1 {
		t.Fatal("newly relevant policy did not enable remote Pod events")
	}
	if err := policies.Delete(updated); err != nil {
		t.Fatal(err)
	}
	c.onNQOSDelete(cache.DeletedFinalStateUnknown{Key: "ns/qos", Obj: updated})
	if c.hasRelevantPolicy.Load() {
		t.Fatal("last policy deletion left Pod events enabled")
	}
}

func TestNamespaceChangesRefreshNetworkQoSRelevance(t *testing.T) {
	c, policies, _, namespaces := newRelevanceTestController(t)
	policy := policyForNAD("a")
	policy.Spec.NetworkSelectors[0].NetworkAttachmentDefinitionSelector.NamespaceSelector = metav1.LabelSelector{MatchLabels: map[string]string{"selected": "true"}}
	if err := policies.Add(policy); err != nil {
		t.Fatal(err)
	}
	c.onNQOSAdd(policy)
	if c.hasRelevantPolicy.Load() {
		t.Fatal("namespace selector should not match yet")
	}
	old := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", ResourceVersion: "1"}}
	for _, selected := range []bool{true, false, true} {
		// Drain policy work so this checks the Namespace event itself schedules
		// resync/cleanup, without requiring any subsequent Pod event.
		for c.nqosQueue.Len() > 0 {
			key, _ := c.nqosQueue.Get()
			c.nqosQueue.Done(key)
		}
		ns := old.DeepCopy()
		ns.ResourceVersion += "1"
		ns.Labels = map[string]string{}
		if selected {
			ns.Labels["selected"] = "true"
		}
		if err := namespaces.Update(ns); err != nil {
			t.Fatal(err)
		}
		c.onNQOSNamespaceUpdate(old, ns)
		if c.hasRelevantPolicy.Load() != selected || c.nqosQueue.Len() != 1 {
			t.Fatalf("namespace change: relevance=%v, queued policies=%d", c.hasRelevantPolicy.Load(), c.nqosQueue.Len())
		}
		old = ns
	}
}

func TestNetworkQoSRelevanceLookupErrorEnablesPodEvents(t *testing.T) {
	c, policies, _, _ := newRelevanceTestController(t)
	policy := policyForNAD("a")
	policy.Spec.NetworkSelectors[0].NetworkAttachmentDefinitionSelector = nil
	if err := policies.Add(policy); err != nil {
		t.Fatal(err)
	}
	c.onNQOSAdd(policy)
	if !c.hasRelevantPolicy.Load() {
		t.Fatal("selector error must preserve Pod event processing")
	}
}

func TestNADDeletionRefreshesNetworkQoSRelevance(t *testing.T) {
	c, policies, nads, _ := newRelevanceTestController(t)
	policy := policyForNAD("a")
	if err := policies.Add(policy); err != nil {
		t.Fatal(err)
	}
	c.onNQOSAdd(policy)
	if !c.hasRelevantPolicy.Load() {
		t.Fatal("expected a relevant policy")
	}
	key, _ := c.nqosQueue.Get()
	c.nqosQueue.Done(key)
	nad, exists, err := nads.GetByKey("ns/network-a")
	if err != nil || !exists {
		t.Fatalf("missing NAD: %v", err)
	}
	if err := nads.Delete(nad); err != nil {
		t.Fatal(err)
	}
	c.onNQOSNADChange(cache.DeletedFinalStateUnknown{Key: "ns/network-a", Obj: nad})
	if c.hasRelevantPolicy.Load() || c.nqosQueue.Len() != 1 {
		t.Fatal("NAD deletion must disable Pod events and schedule policy cleanup")
	}
}

func TestAnyMatchingPolicyEnablesPodEvents(t *testing.T) {
	c, policies, _, _ := newRelevanceTestController(t)
	unrelated := policyForNAD("b")
	matching := policyForNAD("a")
	matching.Name = "matching"
	for _, policy := range []*nqostype.NetworkQoS{unrelated, matching} {
		if err := policies.Add(policy); err != nil {
			t.Fatal(err)
		}
		c.onNQOSAdd(policy)
	}
	if !c.hasRelevantPolicy.Load() {
		t.Fatal("unrelated policies must not hide a matching policy")
	}
	if err := policies.Delete(matching); err != nil {
		t.Fatal(err)
	}
	c.onNQOSDelete(matching)
	if c.hasRelevantPolicy.Load() || !c.hasNetworkQoS() {
		t.Fatal("remaining unrelated policy must not keep Pod events enabled")
	}
}

func TestConcurrentRelevanceRefreshAndPodEvents(t *testing.T) {
	c, policies, _, _ := newRelevanceTestController(t)
	policy := policyForNAD("a")
	if err := policies.Add(policy); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Go(func() {
			for j := 0; j < 100; j++ {
				c.refreshNetworkQoSRelevance()
				c.onNQOSPodAdd(pod)
			}
		})
	}
	workers.Wait()
	if !c.hasRelevantPolicy.Load() || c.nqosPodQueue.Len() != 400 {
		t.Fatal("concurrent refresh lost relevant Pod events")
	}
}

func BenchmarkPodEventsWithUnrelatedNetworkQoS(b *testing.B) {
	c, policies, _, _ := newRelevanceTestController(b)
	if err := policies.Add(policyForNAD("b")); err != nil {
		b.Fatal(err)
	}
	c.refreshNetworkQoSRelevance()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", ResourceVersion: "1"}}
	updated := pod.DeepCopy()
	updated.ResourceVersion = "2"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.onNQOSPodAdd(pod)
		c.onNQOSPodUpdate(pod, updated)
		c.onNQOSPodDelete(updated)
	}
	if c.nqosPodQueue.Len() != 0 {
		b.Fatal("unrelated policy enabled Pod event processing")
	}
}
