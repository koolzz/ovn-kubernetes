// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

// Package namespace provides a level-driven, shared Namespace controller
// that mirrors pkg/controllers/node/NodeController. Per-network handlers
// are registered with the controller; the controller dispatches reconcile
// keys of shape "<ns>|<net>" to the registered handler for net.
//
// This package is in the dormant state: the controller exists, can be
// started, and accepts handler registrations, but no in-tree caller
// registers handlers yet. Migration of pkg/ovn/*Network*Controller to use
// it is gated on later phases (see namespace-migration-plan.md).
package namespace

import (
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/syncmap"
)

// NamespaceHandler handles namespace reconciliation for a single network.
type NamespaceHandler interface {
	// GetNetworkName returns the network this handler reconciles.
	GetNetworkName() string
	// ReconcileNamespace reconciles the network-specific state for a
	// namespace. oldNS and oldState may be nil when the namespace is
	// first seen for the network or becomes active again. For delete
	// reconciliation, oldNS is the best available prior object: a real
	// cached namespace when available, otherwise a name-only stub;
	// oldState is nil when no cached prior annotations exist. newNS and
	// newState describe the latest desired state; they are nil when
	// network-specific state for the namespace should be deleted.
	ReconcileNamespace(oldNS, newNS *corev1.Namespace, oldState, newState *NamespaceAnnotationState) error
	// SyncNamespaces performs the initial full-network sync before
	// per-namespace reconciliation is queued for the handler. Phase 1b
	// added bootstrap obligations for external-gateway state — handlers
	// that program GW routes must seed their applied-state snapshot
	// from NBDB inside SyncNamespaces (or before its return), so the
	// first per-namespace reconcile after restart sees an accurate
	// prior-state.
	SyncNamespaces(namespaces []*corev1.Namespace) error
}

// NamespaceController reconciles namespace state for all registered
// networks. Mirrors NodeController in shape.
type NamespaceController struct {
	name string

	nsController   controller.Controller
	networkManager networkmanager.Interface
	nsLister       v1listers.NamespaceLister

	// handlers maps network name to namespace handler.
	handlers *syncmap.SyncMap[NamespaceHandler]

	// stateMu protects nsReconciliation, nsActive, nsNetworks, nsCache,
	// and latestInformerNsCache.
	stateMu sync.RWMutex
	// nsReconciliation tracks namespaces that should be treated as
	// "new" per network. The bool is true when the next reconcile
	// should be a delete pass (mirrors NodeController.nodeReconciliation).
	// keyed by network -> namespaces
	nsReconciliation map[string]map[string]bool
	// nsActive tracks whether a namespace/network is active.
	// presence indicates active.
	// keyed by network -> namespaces
	nsActive map[string]map[string]struct{}
	// nsNetworks is a reverse index of active networks per namespace.
	// keyed by namespace -> networks
	nsNetworks map[string]map[string]struct{}
	// nsCache contains the last-applied namespace state per network.
	// keyed by network -> nsName
	nsCache map[string]map[string]*corev1.Namespace
	// latestInformerNsCache contains the latest informer object seen
	// for a namespace, even if reconciliation failed. Used as a
	// delete fallback when no configured state exists in nsCache.
	// keyed by network -> nsName
	latestInformerNsCache map[string]map[string]*corev1.Namespace
	// annotationCache stores parsed annotation values keyed by namespace.
	annotationCache *NamespaceAnnotationCache

	startMu sync.Mutex
	started bool
}

const scopedNamespaceQueueKeySeparator = "|"

// NewController builds a shared namespace controller.
func NewController(wf *factory.WatchFactory, name string, networkManager networkmanager.Interface) *NamespaceController {
	if networkManager == nil {
		panic("namespace controller network manager must not be nil")
	}
	nsInformer := wf.NamespaceCoreInformer()
	c := &NamespaceController{
		name:                  name,
		networkManager:        networkManager,
		nsLister:              nsInformer.Lister(),
		handlers:              syncmap.NewSyncMap[NamespaceHandler](),
		nsReconciliation:      map[string]map[string]bool{},
		nsActive:              map[string]map[string]struct{}{},
		nsNetworks:            map[string]map[string]struct{}{},
		nsCache:               map[string]map[string]*corev1.Namespace{},
		latestInformerNsCache: map[string]map[string]*corev1.Namespace{},
		annotationCache:       NewNamespaceAnnotationCache(),
	}

	cfg := &controller.ControllerConfig[corev1.Namespace]{
		RateLimiter:    workqueue.NewTypedItemFastSlowRateLimiter[string](time.Second, 5*time.Second, 5),
		Informer:       nsInformer.Informer(),
		Lister:         nsInformer.Lister().List,
		MaxAttempts:    controller.InfiniteAttempts,
		ObjNeedsUpdate: func(_, _ *corev1.Namespace) bool { return true },
		Reconcile:      c.reconcileNamespace,
		Threadiness:    15,
	}
	c.nsController = controller.NewController(c.name+"-namespace", cfg)

	return c
}

// NewNamespaceController builds a controller that handles namespace events
// for all UDNs.
func NewNamespaceController(wf *factory.WatchFactory, networkManager networkmanager.Interface) *NamespaceController {
	return NewController(wf, "namespace-topology", networkManager)
}

// Start starts the namespace worker.
func (c *NamespaceController) Start() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.started {
		return nil
	}
	if err := controller.Start(c.nsController); err != nil {
		return err
	}
	c.started = true
	return nil
}

// Stop stops the namespace worker.
func (c *NamespaceController) Stop() {
	c.startMu.Lock()
	c.started = false
	c.startMu.Unlock()
	controller.Stop(c.nsController)
}

// ReconcileNetwork queues reconciliation for a single namespace/network
// pair. Mirrors NodeController.ReconcileNetwork.
func (c *NamespaceController) ReconcileNetwork(nsName, netName string) {
	c.nsController.Reconcile(scopedNamespaceQueueKey(nsName, netName))
}

// AnnotationCache returns the cache used for parsed namespace annotations.
func (c *NamespaceController) AnnotationCache() *NamespaceAnnotationCache {
	return c.annotationCache
}

// RegisterNetworkController registers or replaces a per-network namespace
// handler. Registration triggers a bootstrap pass for the network: every
// known namespace is sync'd via the handler's SyncNamespaces and then
// queued for an individual reconcile.
func (c *NamespaceController) RegisterNetworkController(handler NamespaceHandler) error {
	if handler == nil {
		return fmt.Errorf("%s: nil namespace handler registration", c.name)
	}
	netName := handler.GetNetworkName()
	return c.handlers.DoWithLock(netName, func(key string) error {
		if existing, ok := c.handlers.Load(key); ok && existing != nil {
			panic(fmt.Sprintf("%s: duplicate namespace handler registration for network %q", c.name, key))
		}
		c.handlers.Store(key, handler)
		if err := c.bootstrapNetwork(key, handler); err != nil {
			c.handlers.Delete(key)
			return fmt.Errorf("%s: failed to bootstrap network %s: %w", c.name, netName, err)
		}
		return nil
	})
}

// DeregisterNetworkController removes a per-network namespace handler and
// clears associated network state. OVN cleanup for namespaces is the
// handler's responsibility (typically driven by a final delete-reconcile
// pass before deregister).
func (c *NamespaceController) DeregisterNetworkController(netName string) {
	_ = c.handlers.DoWithLock(netName, func(key string) error {
		c.handlers.Delete(key)
		c.stateMu.Lock()
		delete(c.nsReconciliation, key)
		if nss, ok := c.nsActive[key]; ok {
			for nsName := range nss {
				if networks, ok := c.nsNetworks[nsName]; ok {
					delete(networks, key)
					if len(networks) == 0 {
						delete(c.nsNetworks, nsName)
					}
				}
			}
		}
		delete(c.nsActive, key)
		delete(c.nsCache, key)
		delete(c.latestInformerNsCache, key)
		c.stateMu.Unlock()
		return nil
	})
}

// reconcileNamespace handles namespace add/update/delete by comparing
// cached state.
func (c *NamespaceController) reconcileNamespace(key string) error {
	nsName, netName := parseScopedNamespaceQueueKey(key)
	// An empty netName fans out to every registered network.
	if netName == "" {
		for _, networkName := range c.handlers.GetKeys() {
			c.ReconcileNetwork(nsName, networkName)
		}
		return nil
	}

	return c.handlers.DoWithLock(netName, func(handlerKey string) error {
		handler, ok := c.handlers.Load(handlerKey)
		if !ok || handler == nil {
			return nil
		}

		needsDelete := c.namespaceNeedsDeleteReconciliation(netName, nsName)
		needsAddUpdate := true

		newNS, err := c.nsLister.Get(nsName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		updateAnnoCacheOnDelete := false
		if newNS == nil {
			needsDelete = true
			needsAddUpdate = false
			updateAnnoCacheOnDelete = true
		}

		oldNS := c.getCachedNamespace(netName, nsName)
		var oldState *NamespaceAnnotationState
		if oldNS != nil || updateAnnoCacheOnDelete {
			oldState = c.annotationCache.UpdateNamespaceAnnotationState(oldNS, updateAnnoCacheOnDelete)
		}

		if needsDelete {
			if err := c.reconcileDelete(handler, nsName, netName, oldNS, oldState); err != nil {
				return fmt.Errorf("%s: failed to delete namespace %s for network %s: %w", c.name, nsName, netName, err)
			}
		}

		if !needsAddUpdate || newNS == nil {
			c.deleteNamespaceReconciliation(netName, nsName)
			return nil
		}

		newState := c.annotationCache.UpdateNamespaceAnnotationState(newNS, true)

		// If we've been marked for reconciliation (first-time-active or
		// bootstrap), treat it as a fresh add by clearing the prior view.
		if c.namespaceNeedsReconciliation(netName, nsName) {
			oldNS = nil
			oldState = nil
		}
		c.setNamespaceActive(netName, nsName)
		return c.reconcileUpdate(handler, oldNS, newNS, netName, oldState, newState)
	})
}

func (c *NamespaceController) reconcileUpdate(handler NamespaceHandler, oldNS, newNS *corev1.Namespace, netName string, oldState, newState *NamespaceAnnotationState) error {
	err := handler.ReconcileNamespace(oldNS, newNS, oldState, newState)
	c.setLatestInformerNamespace(netName, newNS)
	if err != nil {
		return err
	}
	c.setCachedNamespace(netName, newNS)
	c.deleteNamespaceReconciliation(netName, newNS.Name)
	return nil
}

func (c *NamespaceController) reconcileDelete(handler NamespaceHandler, nsName, netName string, oldNS *corev1.Namespace, oldState *NamespaceAnnotationState) error {
	if oldNS == nil {
		oldNS = c.getLatestInformerNamespace(netName, nsName)
		if oldNS != nil {
			oldState = c.annotationCache.UpdateNamespaceAnnotationState(oldNS, false)
		}
	}
	if oldNS == nil {
		oldNS = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	}
	if err := handler.ReconcileNamespace(oldNS, nil, oldState, nil); err != nil {
		return err
	}

	c.deleteNamespaceActive(netName, nsName)
	c.clearNamespaceDeleteReconciliation(netName, nsName)
	c.deleteCachedNamespace(netName, nsName)
	c.deleteLatestInformerNamespace(netName, nsName)

	if c.namespaceHasAnyNetwork(nsName) {
		return nil
	}
	c.annotationCache.DeleteNamespace(nsName)
	return nil
}

func (c *NamespaceController) bootstrapNetwork(netName string, handler NamespaceHandler) error {
	nss, err := c.nsLister.List(labels.Everything())
	if err != nil {
		return err
	}
	if err := handler.SyncNamespaces(nss); err != nil {
		return err
	}
	c.setBootstrapNamespaces(netName, nss)
	for _, ns := range nss {
		c.nsController.Reconcile(scopedNamespaceQueueKey(ns.Name, netName))
	}
	return nil
}

func (c *NamespaceController) setBootstrapNamespaces(netName string, nss []*corev1.Namespace) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if len(nss) == 0 {
		return
	}
	out := make(map[string]bool, len(nss))
	for _, ns := range nss {
		out[ns.Name] = false
	}
	c.nsReconciliation[netName] = out
}

func (c *NamespaceController) namespaceNeedsReconciliation(netName, nsName string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	nss := c.nsReconciliation[netName]
	if len(nss) == 0 {
		return false
	}
	_, ok := nss[nsName]
	return ok
}

func (c *NamespaceController) namespaceNeedsDeleteReconciliation(netName, nsName string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	nss := c.nsReconciliation[netName]
	if len(nss) == 0 {
		return false
	}
	if needsDelete, ok := nss[nsName]; ok && needsDelete {
		return true
	}
	return false
}

func (c *NamespaceController) clearNamespaceDeleteReconciliation(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.nsReconciliation[netName]
	if len(nss) == 0 {
		return
	}
	if _, ok := nss[nsName]; ok {
		nss[nsName] = false
	}
}

func (c *NamespaceController) deleteNamespaceReconciliation(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.nsReconciliation[netName]
	if len(nss) == 0 {
		return
	}
	delete(nss, nsName)
	if len(nss) == 0 {
		delete(c.nsReconciliation, netName)
	}
}

// MarkNamespaceNeedsReconciliation flags the namespace as needing a
// reconciliation pass for the given network. Callers use this when a NAD
// transition or similar makes a previously-unactive namespace active for
// the network.
func (c *NamespaceController) MarkNamespaceNeedsReconciliation(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.nsReconciliation[netName]
	if nss == nil {
		nss = map[string]bool{}
		c.nsReconciliation[netName] = nss
	}
	if _, ok := nss[nsName]; ok {
		return
	}
	nss[nsName] = false
}

// MarkNamespaceNeedsDeleteReconciliation flags the namespace as needing a
// delete pass for the given network. Callers use this when a NAD
// transition deactivates a previously-active namespace for the network.
// The next reconcile will run handler.ReconcileNamespace with newNS=nil so
// the handler can tear down its OVN-side state.
func (c *NamespaceController) MarkNamespaceNeedsDeleteReconciliation(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.nsReconciliation[netName]
	if nss == nil {
		nss = map[string]bool{}
		c.nsReconciliation[netName] = nss
	}
	nss[nsName] = true
}

func (c *NamespaceController) namespaceHasAnyNetwork(nsName string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return len(c.nsNetworks[nsName]) > 0
}

func (c *NamespaceController) setNamespaceActive(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.nsActive == nil {
		c.nsActive = map[string]map[string]struct{}{}
	}
	if c.nsNetworks == nil {
		c.nsNetworks = map[string]map[string]struct{}{}
	}
	nss := c.nsActive[netName]
	if nss == nil {
		nss = map[string]struct{}{}
		c.nsActive[netName] = nss
	}
	nss[nsName] = struct{}{}
	networks := c.nsNetworks[nsName]
	if networks == nil {
		networks = map[string]struct{}{}
		c.nsNetworks[nsName] = networks
	}
	networks[netName] = struct{}{}
}

func (c *NamespaceController) deleteNamespaceActive(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if nss, ok := c.nsActive[netName]; ok {
		delete(nss, nsName)
		if len(nss) == 0 {
			delete(c.nsActive, netName)
		}
	}
	if networks, ok := c.nsNetworks[nsName]; ok {
		delete(networks, netName)
		if len(networks) == 0 {
			delete(c.nsNetworks, nsName)
		}
	}
}

func scopedNamespaceQueueKey(nsName, netName string) string {
	if netName == "" {
		return nsName
	}
	return nsName + scopedNamespaceQueueKeySeparator + netName
}

func parseScopedNamespaceQueueKey(key string) (nsName, netName string) {
	parts := strings.SplitN(key, scopedNamespaceQueueKeySeparator, 2)
	if len(parts) != 2 {
		return key, ""
	}
	if parts[0] == "" || parts[1] == "" {
		return key, ""
	}
	return parts[0], parts[1]
}

func (c *NamespaceController) getCachedNamespace(netName, nsName string) *corev1.Namespace {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	ns := c.nsCache[netName][nsName]
	if ns == nil {
		return nil
	}
	return ns.DeepCopy()
}

func (c *NamespaceController) setCachedNamespace(netName string, ns *corev1.Namespace) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.nsCache[netName] == nil {
		c.nsCache[netName] = map[string]*corev1.Namespace{}
	}
	c.nsCache[netName][ns.Name] = ns.DeepCopy()
}

func (c *NamespaceController) deleteCachedNamespace(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.nsCache[netName]
	if nss == nil {
		return
	}
	delete(nss, nsName)
	if len(nss) == 0 {
		delete(c.nsCache, netName)
	}
}

func (c *NamespaceController) getLatestInformerNamespace(netName, nsName string) *corev1.Namespace {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	ns := c.latestInformerNsCache[netName][nsName]
	if ns == nil {
		return nil
	}
	return ns.DeepCopy()
}

func (c *NamespaceController) setLatestInformerNamespace(netName string, ns *corev1.Namespace) {
	if ns == nil {
		return
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.latestInformerNsCache[netName] == nil {
		c.latestInformerNsCache[netName] = map[string]*corev1.Namespace{}
	}
	c.latestInformerNsCache[netName][ns.Name] = ns.DeepCopy()
}

func (c *NamespaceController) deleteLatestInformerNamespace(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	nss := c.latestInformerNsCache[netName]
	if nss == nil {
		return
	}
	delete(nss, nsName)
	if len(nss) == 0 {
		delete(c.latestInformerNsCache, netName)
	}
}
