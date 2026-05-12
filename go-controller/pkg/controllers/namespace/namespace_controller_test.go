// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"errors"
	"strings"
	"testing"
	"time"

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
		bootstrapPending:      map[string]map[string]struct{}{},
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
		// Empty namespace name must roundtrip too — kubevirt tests
		// historically create namespace fixtures with zero-valued names,
		// and a non-roundtripping parse would loop the fan-out
		// indefinitely ("|net" → bare → enqueue "|net|net" → ...).
		{"", "default"},
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

func TestWaitForBootstrap(t *testing.T) {
	c := newTestController(t)
	// Empty bootstrapPending drains immediately.
	if err := c.WaitForBootstrap("default", 100*time.Millisecond); err != nil {
		t.Fatalf("empty bootstrapPending should drain immediately; got %v", err)
	}
	// A pending bootstrap entry blocks until drained; the timeout
	// expires here because nothing clears it.
	c.stateMu.Lock()
	c.bootstrapPending["default"] = map[string]struct{}{"alpha": {}}
	c.stateMu.Unlock()
	if err := c.WaitForBootstrap("default", 50*time.Millisecond); err == nil {
		t.Fatal("WaitForBootstrap should time out while bootstrapPending has entries")
	}
	// markBootstrapAttempted lets it drain (the public path; equivalent
	// to what reconcileNamespace's defer does).
	c.markBootstrapAttempted("default", "alpha")
	if err := c.WaitForBootstrap("default", 100*time.Millisecond); err != nil {
		t.Fatalf("after markBootstrapAttempted, bootstrap should drain; got %v", err)
	}
}

func TestWaitForBootstrap_DrainsOnHandlerError(t *testing.T) {
	// Regression: previously the bootstrap entry was cleared only on
	// SUCCESSFUL handler return, so one transient reconcile error
	// (e.g., a malformed external-gateway annotation crashing parse, a
	// transient NBDB hiccup) would brick controller startup after the
	// 30s timeout. WaitForBootstrap must now return as soon as every
	// bootstrap namespace has been ATTEMPTED, regardless of outcome —
	// failed namespaces stay in the workqueue's retry path with normal
	// backoff but no longer block dependent watchers.
	c := newTestController(t)
	wantErr := errors.New("transient handler failure")
	h := &fakeNamespaceHandler{netName: "default", reconcileErr: wantErr}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alpha"}}
	c.nsLister = newNamespaceLister(t, ns)
	c.handlers.Store("default", h)
	c.setBootstrapNamespaces("default", []*corev1.Namespace{ns})

	// Sanity: the namespace is pending before any reconcile attempt.
	c.stateMu.RLock()
	if _, pending := c.bootstrapPending["default"]["alpha"]; !pending {
		c.stateMu.RUnlock()
		t.Fatal("test premise: bootstrapPending must contain alpha before the reconcile")
	}
	c.stateMu.RUnlock()

	// Run the reconcile. The handler returns an error, so the
	// reconcileNamespace return propagates the error to the workqueue
	// for retry. But the bootstrap entry MUST be cleared by the defer.
	err := c.reconcileNamespace(scopedNamespaceQueueKey("alpha", "default"))
	if err == nil {
		t.Fatal("expected handler error to propagate to the workqueue")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}

	// WaitForBootstrap must return without timing out — the namespace
	// has been ATTEMPTED, that's all the drain cares about.
	if err := c.WaitForBootstrap("default", 100*time.Millisecond); err != nil {
		t.Fatalf("bootstrap should drain after one attempt even on handler error; got %v", err)
	}

	// nsReconciliation entry persists (still needs the Mark*
	// fresh-add semantic on the next retry) — only bootstrapPending
	// gets cleared by the attempt.
	c.stateMu.RLock()
	if _, stillPending := c.nsReconciliation["default"]["alpha"]; !stillPending {
		t.Error("nsReconciliation entry must persist on handler error so the retry treats it as fresh-add")
	}
	c.stateMu.RUnlock()
}

func TestWaitForBootstrap_TimeoutErrorIncludesSample(t *testing.T) {
	// The timeout error message must include a sample of pending
	// namespaces so operators debugging a hung startup can see which
	// namespaces never had a worker pick them up.
	c := newTestController(t)
	c.stateMu.Lock()
	c.bootstrapPending["default"] = map[string]struct{}{
		"alpha": {}, "beta": {}, "gamma": {},
	}
	c.stateMu.Unlock()
	err := c.WaitForBootstrap("default", 30*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "3 namespaces pending") {
		t.Errorf("error must include count of pending namespaces: %q", msg)
	}
	// At least one of the pending names should appear in the message
	// (we don't assert exact ordering since map iteration is unordered).
	hasSample := false
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if strings.Contains(msg, name) {
			hasSample = true
			break
		}
	}
	if !hasSample {
		t.Errorf("error must include at least one pending namespace name: %q", msg)
	}
}

func TestReconcileNamespace_EmptyNameIsNoop(t *testing.T) {
	// scopedNamespaceQueueKey("", "default") encodes as "|default";
	// parseScopedNamespaceQueueKey returns ("", "default"); reconcile
	// short-circuits on the empty nsName so fan-out doesn't loop and
	// no handler is invoked. Asserting nil return (no error) is the
	// observable contract.
	c := newTestController(t)
	h := &fakeNamespaceHandler{netName: "default"}
	if err := c.RegisterNetworkController(h); err != nil {
		t.Fatalf("RegisterNetworkController failed: %v", err)
	}
	if err := c.reconcileNamespace(scopedNamespaceQueueKey("", "default")); err != nil {
		t.Fatalf("reconcileNamespace on empty-name key should be a no-op; got %v", err)
	}
	if h.reconcileCalls != 0 {
		t.Fatalf("handler.ReconcileNamespace should not have been called for empty-name key; got %d calls", h.reconcileCalls)
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

func TestBootstrapNetwork_FiltersEmptyNameNamespaces(t *testing.T) {
	// Empty-name namespaces (test fixtures only — Kubernetes rejects
	// them in production) must NOT be enqueued during bootstrap, and
	// must NOT contribute to nsReconciliation. scopedNamespaceQueueKey
	// produces "|net" for empty nsName which the parser then loops on
	// during fan-out; the bootstrap-side filter is the upstream
	// defense for the reconcileNamespace short-circuit.
	c := newTestController(t)
	c.nsLister = newNamespaceLister(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "real-ns"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ""}},
	)
	h := &fakeNamespaceHandler{netName: "net-a"}
	if err := c.RegisterNetworkController(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	// SyncNamespaces is called with the unfiltered list (it's the
	// handler's call); the bootstrap drain set is what matters.
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	pending := c.nsReconciliation["net-a"]
	if _, ok := pending[""]; ok {
		t.Fatalf("empty-name namespace must not be in nsReconciliation; got %v", pending)
	}
	if _, ok := pending["real-ns"]; !ok {
		t.Fatalf("real-ns must be in nsReconciliation; got %v", pending)
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
