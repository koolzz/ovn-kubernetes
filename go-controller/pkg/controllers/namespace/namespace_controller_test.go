// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/syncmap"
)

type fakeNamespaceHandler struct {
	netName        string
	syncErr        error
	reconcileErr   error
	syncCalls      int
	reconcileCalls int
	deleteCalls    int
	lastOldNS      *corev1.Namespace
}

func (f *fakeNamespaceHandler) GetNetworkName() string { return f.netName }

func (f *fakeNamespaceHandler) ReconcileNamespace(oldNS, newNS *corev1.Namespace, _, _ *NamespaceAnnotationState) error {
	if oldNS != nil {
		f.lastOldNS = oldNS.DeepCopy()
	} else {
		f.lastOldNS = nil
	}
	if newNS == nil {
		f.deleteCalls++
		return nil
	}
	f.reconcileCalls++
	return f.reconcileErr
}

func (f *fakeNamespaceHandler) SyncNamespaces(_ []*corev1.Namespace) error {
	f.syncCalls++
	return f.syncErr
}

func newNamespaceLister(t *testing.T, nss ...*corev1.Namespace) corelisters.NamespaceLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, ns := range nss {
		if err := indexer.Add(ns); err != nil {
			t.Fatalf("failed to add namespace to indexer: %v", err)
		}
	}
	return corelisters.NewNamespaceLister(indexer)
}

func newNamespaceControllerForTest(threadiness int) controller.Controller {
	return controller.NewController("topology-test-namespace-controller", &controller.ControllerConfig[corev1.Namespace]{
		RateLimiter:    workqueue.DefaultTypedControllerRateLimiter[string](),
		Reconcile:      func(string) error { return nil },
		Threadiness:    threadiness,
		Lister:         func(labels.Selector) ([]*corev1.Namespace, error) { return nil, nil },
		ObjNeedsUpdate: func(_, _ *corev1.Namespace) bool { return true },
	})
}

func newTestController(t *testing.T) *NamespaceController {
	t.Helper()
	return &NamespaceController{
		name:                  "topology-test",
		networkManager:        networkmanager.Default().Interface(),
		nsLister:              newNamespaceLister(t),
		handlers:              syncmap.NewSyncMap[NamespaceHandler](),
		nsReconciliation:      map[string]map[string]bool{},
		nsActive:              map[string]map[string]struct{}{},
		nsNetworks:            map[string]map[string]struct{}{},
		nsCache:               map[string]map[string]*corev1.Namespace{},
		latestInformerNsCache: map[string]map[string]*corev1.Namespace{},
		annotationCache:       NewNamespaceAnnotationCache(),
		nsController:          newNamespaceControllerForTest(0),
	}
}

func TestScopedNamespaceQueueKeyRoundtrip(t *testing.T) {
	cases := []struct{ ns, net string }{
		{"alpha", "default"},
		{"beta", "tenant-a"},
		{"with-dash", "udn-1"},
	}
	for _, tc := range cases {
		key := scopedNamespaceQueueKey(tc.ns, tc.net)
		ns, net := parseScopedNamespaceQueueKey(key)
		if ns != tc.ns || net != tc.net {
			t.Fatalf("roundtrip for (%q,%q) yielded (%q,%q)", tc.ns, tc.net, ns, net)
		}
	}
}

func TestParseScopedNamespaceQueueKey_FanOut(t *testing.T) {
	// An empty net name signals fan-out: the parser returns net == "".
	ns, net := parseScopedNamespaceQueueKey("alpha")
	if ns != "alpha" || net != "" {
		t.Fatalf("unexpected fan-out parse: ns=%q net=%q", ns, net)
	}
}

func TestRegisterDeregisterNetworkController(t *testing.T) {
	c := newTestController(t)
	h := &fakeNamespaceHandler{netName: "net-a"}

	if err := c.RegisterNetworkController(h); err != nil {
		t.Fatalf("RegisterNetworkController failed: %v", err)
	}
	if h.syncCalls != 1 {
		t.Fatalf("expected SyncNamespaces to be called once on register; got %d", h.syncCalls)
	}

	if got, ok := c.handlers.Load("net-a"); !ok || got != h {
		t.Fatalf("handler not stored after register")
	}

	c.DeregisterNetworkController("net-a")
	if _, ok := c.handlers.Load("net-a"); ok {
		t.Fatalf("handler still present after deregister")
	}
}

func TestRegisterNetworkControllerBootstrapFailure(t *testing.T) {
	c := newTestController(t)
	h := &fakeNamespaceHandler{netName: "net-a", syncErr: errors.New("sync failed")}

	err := c.RegisterNetworkController(h)
	if err == nil {
		t.Fatal("expected RegisterNetworkController to return the bootstrap error")
	}
	if _, ok := c.handlers.Load("net-a"); ok {
		t.Fatal("handler must not be retained when bootstrap fails")
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	c := newTestController(t)
	h := &fakeNamespaceHandler{netName: "net-a"}
	if err := c.RegisterNetworkController(h); err != nil {
		t.Fatalf("first register should succeed: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected duplicate registration to panic")
		}
	}()
	_ = c.RegisterNetworkController(&fakeNamespaceHandler{netName: "net-a"})
}

func TestRegisterIsNilSafe(t *testing.T) {
	c := newTestController(t)
	if err := c.RegisterNetworkController(nil); err == nil {
		t.Fatal("expected nil-handler registration to error")
	}
}

func TestMarkNamespaceNeedsReconciliationFlags(t *testing.T) {
	c := newTestController(t)

	// Activity-flip: namespace becomes active, then the next reconcile
	// should treat it as new (not delete).
	c.MarkNamespaceNeedsReconciliation("net-a", "ns1")
	if !c.namespaceNeedsReconciliation("net-a", "ns1") {
		t.Fatalf("expected ns1 to be marked needsReconciliation for net-a")
	}
	if c.namespaceNeedsDeleteReconciliation("net-a", "ns1") {
		t.Fatalf("ns1 must not be marked needsDelete after MarkNeedsReconciliation")
	}

	// Activity-flip in the other direction: namespace becomes inactive,
	// next reconcile should run delete.
	c.MarkNamespaceNeedsDeleteReconciliation("net-a", "ns2")
	if !c.namespaceNeedsDeleteReconciliation("net-a", "ns2") {
		t.Fatalf("expected ns2 to be marked needsDelete for net-a")
	}
}

func TestSetAndDeleteNamespaceActive(t *testing.T) {
	c := newTestController(t)
	c.setNamespaceActive("net-a", "ns1")
	c.setNamespaceActive("net-b", "ns1")

	if !c.namespaceHasAnyNetwork("ns1") {
		t.Fatal("ns1 should be active on at least one network")
	}

	c.deleteNamespaceActive("net-a", "ns1")
	if !c.namespaceHasAnyNetwork("ns1") {
		t.Fatal("ns1 should still be active on net-b after removing net-a")
	}
	c.deleteNamespaceActive("net-b", "ns1")
	if c.namespaceHasAnyNetwork("ns1") {
		t.Fatal("ns1 should not be active on any network after both removed")
	}
}

func TestCachedNamespaceRoundtrip(t *testing.T) {
	c := newTestController(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}}

	if got := c.getCachedNamespace("net-a", "ns1"); got != nil {
		t.Fatal("expected nil for unknown cache lookup")
	}
	c.setCachedNamespace("net-a", ns)
	got := c.getCachedNamespace("net-a", "ns1")
	if got == nil || got.Name != "ns1" {
		t.Fatalf("expected cached namespace name=ns1, got %#v", got)
	}
	// Mutating the returned object must not affect the cache (deep-copy).
	got.Annotations = map[string]string{"injected": "yes"}
	again := c.getCachedNamespace("net-a", "ns1")
	if _, present := again.Annotations["injected"]; present {
		t.Fatal("mutation of returned namespace leaked into cache")
	}
	c.deleteCachedNamespace("net-a", "ns1")
	if c.getCachedNamespace("net-a", "ns1") != nil {
		t.Fatal("expected nil after deleteCachedNamespace")
	}
}
