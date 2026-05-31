// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

// Package namespace provides a level-driven, shared Namespace controller
// that mirrors pkg/controllers/node/NodeController. Per-network handlers
// are registered with the controller; the controller dispatches reconcile
// keys of shape "<ns>|<net>" to the registered handler for net.
//
// In-tree consumers: DefaultNetworkController plus all three UDN topology
// controllers (Layer3, Layer2, Localnet) register their NamespaceHandler
// implementations through this controller via
// BaseNetworkController.registerNamespaceReconciler. The legacy
// retryNamespaces watch was removed once all four were migrated; see
// namespace-migration-plan.md for the full migration timeline.
//
// Key contracts:
//
//   - The transition gate in reconcileNamespace makes membership
//     decisions before dispatch via handler.ClaimsNamespace, so a
//     namespace moving between handlers (NAD change) reaches the right
//     delete leg without the previous owner's state leaking.
//   - WaitForBootstrap blocks until every namespace enqueued by
//     bootstrapNetwork has had its FIRST reconcile attempt; failed
//     reconciles stay in the workqueue's retry path but DON'T block
//     dependent watchers' startup.
//   - registerNamespaceReconciler in pkg/ovn folds Register +
//     WaitForBootstrap so dependent watchers (NetworkPolicy, Pods) see
//     a processed namespace cache, matching the legacy WatchNamespaces
//     ordering contract.
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
	// ClaimsNamespace reports whether this handler would program any
	// OVN state for nsName. The shared controller calls it before
	// dispatch to detect membership transitions (e.g. a NAD change
	// moving a namespace between UDNs) and to decide which leg of the
	// reconcile to run.
	//
	// Returns:
	//   - (true,  nil)  — namespace belongs to this handler's network.
	//   - (false, nil)  — namespace does not belong (filtered).
	//   - (_,     err)  — predicate could not be evaluated (e.g.
	//     transient cache miss). The controller must NOT mutate active
	//     or cache state on this path; the reconcile is requeued.
	//
	// Implementations must be cheap and side-effect-free.
	ClaimsNamespace(nsName string) (bool, error)
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

	// stateMu protects nsReconciliation, bootstrapPending, nsActive,
	// nsNetworks, nsCache, and latestInformerNsCache.
	stateMu sync.RWMutex
	// nsReconciliation tracks namespaces that should be treated as a
	// fresh add for a network on their next reconcile (the gate nulls
	// oldNS when an entry is present). Populated by bootstrapNetwork;
	// cleared on a SUCCESSFUL reconcile.
	//
	// Unlike NodeController.nodeReconciliation, there is no delete-pending
	// dimension: NodeController eagerly clears active state before its
	// delete handler runs and needs a persistent mark to re-drive a
	// failed delete on retry. Here deleteNamespaceActive runs only AFTER
	// the handler delete succeeds (see reconcileDelete), so the cached
	// "had" state survives a failed delete and the (had && !has) branch
	// re-drives the retry on its own — no mark needed.
	// keyed by network -> namespaces
	nsReconciliation map[string]map[string]struct{}
	// bootstrapPending tracks namespaces enqueued by bootstrapNetwork
	// that haven't yet had their FIRST reconcile attempt complete (for
	// any outcome, success or error). WaitForBootstrap watches this
	// set, not nsReconciliation: a persistently-failing namespace must
	// stay in the workqueue retry path but MUST NOT block dependent
	// watchers' startup (legacy WatchNamespaces left failures in retry
	// and returned).
	// keyed by network -> namespaces
	bootstrapPending map[string]map[string]struct{}
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
		nsReconciliation:      map[string]map[string]struct{}{},
		bootstrapPending:      map[string]map[string]struct{}{},
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

// WaitForBootstrap blocks until every namespace enqueued by
// bootstrapNetwork(netName) has had its FIRST reconcile attempt
// complete (regardless of success), or the deadline elapses.
//
// The semantic is "attempted at least once," not "successfully
// applied." A persistently-failing namespace stays in the workqueue
// retry path with normal backoff; WaitForBootstrap returns as soon as
// every bootstrap namespace has been picked up by a worker. Legacy
// WatchNamespaces had the same contract — it enqueued initial adds,
// left failures in retry, and returned. Without this split, a single
// transient handler error (e.g. an in-flight NBDB hiccup or a
// malformed external-gateway annotation that fails parse) would brick
// controller startup after the 30s timeout.
//
// Used by callers that need to know per-namespace setup has been
// PROCESSED once (so dependent watchers like NetworkPolicy don't race
// the namespace's add path), not callers that need every namespace to
// be in a known-good state.
func (c *NamespaceController) WaitForBootstrap(netName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		c.stateMu.RLock()
		pendingCount := len(c.bootstrapPending[netName])
		var sample []string
		if pendingCount > 0 {
			// Capture up to 5 pending namespaces for the timeout
			// error message — operators debugging a hung startup
			// need to know which namespaces never got picked up.
			for ns := range c.bootstrapPending[netName] {
				sample = append(sample, ns)
				if len(sample) == 5 {
					break
				}
			}
		}
		c.stateMu.RUnlock()
		if pendingCount == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: bootstrap drain for network %q exceeded %s (%d namespaces pending; sample: %v)",
				c.name, netName, timeout, pendingCount, sample)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
		delete(c.bootstrapPending, key)
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
// the cached "applied" state to the handler's current claim, and
// running the right leg (add / update / delete / no-op).
//
// Transition gate, mirroring NodeController.reconcileNode:
//
//   - newNS == nil (Kubernetes namespace deleted): membership is
//     definitionally false. Don't consult the predicate (its data
//     sources may depend on the now-missing namespace). Run the delete
//     leg only if we had applied state.
//   - handler.ClaimsNamespace errors: requeue without mutating active
//     or cache state. Treating a transient lookup failure as
//     "doesn't claim" would spuriously delete state on bad data.
//   - (had, has) matrix:
//     !had && !has: not ours, never was. No dispatch, no caching.
//     !had &&  has: inactive → active. Fresh-add semantics.
//     had && !has: active → inactive (e.g. NAD change moved the
//     namespace out of this handler's scope). Delete
//     leg, clear cache + active.
//     had &&  has: normal update with cached prior view.
//
// The handler-level membership predicate (ClaimsNamespace) is the one
// intentional divergence from NodeController, which sources the
// equivalent fact from networkmanager.NodeHasNetwork. Namespace
// membership isn't a single network-manager-level fact today (default
// network claims all; primary UDN claims by NAD; secondary UDN with
// multi-netpol claims by yet another rule), so the predicate stays on
// the handler.
func (c *NamespaceController) reconcileNamespace(key string) error {
	nsName, netName := parseScopedNamespaceQueueKey(key)
	// Empty namespace names are not valid Kubernetes namespaces; an
	// informer-driven Add with an empty-name namespace would loop the
	// fan-out below indefinitely (scopedNamespaceQueueKey("", net)
	// produces "|net", which parses with an empty nsName, fans out,
	// re-encodes as "|net|net", etc.). Skip such keys at the source.
	if nsName == "" {
		return nil
	}
	// An empty netName fans out to every registered network.
	if netName == "" {
		for _, networkName := range c.handlers.GetKeys() {
			c.ReconcileNetwork(nsName, networkName)
		}
		return nil
	}

	return c.handlers.DoWithLock(netName, func(handlerKey string) error {
		// markBootstrapAttempted is called on EVERY path out of this
		// callback so WaitForBootstrap unblocks once every bootstrap
		// namespace has been picked up by a worker, even if the
		// reconcile errored. See WaitForBootstrap's doc for the
		// availability rationale. Idempotent for non-bootstrap
		// namespaces and re-attempts.
		defer c.markBootstrapAttempted(netName, nsName)

		handler, ok := c.handlers.Load(handlerKey)
		if !ok || handler == nil {
			return nil
		}

		newNS, err := c.nsLister.Get(nsName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		oldNS := c.getCachedNamespace(netName, nsName)
		had := c.namespaceHasNetwork(netName, nsName)

		// Deletion special-case. Skip the predicate — namespace is
		// gone and ClaimsNamespace's data sources may be unreliable.
		if newNS == nil {
			var oldState *NamespaceAnnotationState
			if oldNS != nil {
				oldState = c.annotationCache.UpdateNamespaceAnnotationState(oldNS, true)
			}
			if had {
				if err := c.reconcileDelete(handler, nsName, netName, oldNS, oldState); err != nil {
					return fmt.Errorf("%s: failed to delete namespace %s for network %s: %w", c.name, nsName, netName, err)
				}
			}
			c.deleteNamespaceReconciliation(netName, nsName)
			return nil
		}

		has, claimErr := handler.ClaimsNamespace(nsName)
		if claimErr != nil {
			// No active or cache mutation on the error path; the
			// workqueue requeues and the next pass will retry.
			return fmt.Errorf("%s: ClaimsNamespace(%q) for network %q: %w", c.name, nsName, netName, claimErr)
		}

		switch {
		case !had && !has:
			// Not ours, never was. Drain the bootstrap entry (if
			// any) and return.
			c.deleteNamespaceReconciliation(netName, nsName)
			return nil

		case had && !has:
			// Active → inactive. Closes the NAD-transition gap:
			// without this branch, the legacy code path would have
			// run a normal update, the handler would have filtered
			// and returned nil, and the OVN state programmed for
			// the previous network would have leaked.
			var oldState *NamespaceAnnotationState
			if oldNS != nil {
				oldState = c.annotationCache.UpdateNamespaceAnnotationState(oldNS, false)
			}
			if err := c.reconcileDelete(handler, nsName, netName, oldNS, oldState); err != nil {
				return fmt.Errorf("%s: failed to delete namespace %s for network %s: %w", c.name, nsName, netName, err)
			}
			c.deleteNamespaceReconciliation(netName, nsName)
			return nil

		case !had && has:
			// Inactive → active: force-add semantics. Clear the
			// cached prior view so the handler sees a clean add.
			oldNS = nil
		}

		// Fall through: had && has, or force-add from case 3 above.
		// A bootstrap namespace (seeded into nsReconciliation by
		// bootstrapNetwork) is treated as a fresh add even if it
		// raced an informer event that already marked it active.
		if c.namespaceNeedsReconciliation(netName, nsName) {
			oldNS = nil
		}

		var oldState *NamespaceAnnotationState
		if oldNS != nil {
			oldState = c.annotationCache.UpdateNamespaceAnnotationState(oldNS, false)
		}
		newState := c.annotationCache.UpdateNamespaceAnnotationState(newNS, true)

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
	// Empty-name namespaces (test-only fixtures; Kubernetes rejects
	// them in production) can't roundtrip through the scoped queue key
	// encoding and would never be reconciled by the worker, leaving
	// the bootstrap drain hanging. Filter them out of both the
	// outstanding-reconciliation set and the enqueue pass.
	valid := nss[:0]
	for _, ns := range nss {
		if ns.Name == "" {
			continue
		}
		valid = append(valid, ns)
	}
	c.setBootstrapNamespaces(netName, valid)
	for _, ns := range valid {
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
	recon := make(map[string]struct{}, len(nss))
	pending := make(map[string]struct{}, len(nss))
	for _, ns := range nss {
		recon[ns.Name] = struct{}{}
		pending[ns.Name] = struct{}{}
	}
	c.nsReconciliation[netName] = recon
	c.bootstrapPending[netName] = pending
}

// markBootstrapAttempted records that nsName has had its first
// reconcile attempt for netName. Idempotent — calls for namespaces
// not in the bootstrap set, or already-attempted namespaces, are
// no-ops. Called from reconcileNamespace via defer so the mark fires
// for every attempt regardless of outcome.
func (c *NamespaceController) markBootstrapAttempted(netName, nsName string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	pending, ok := c.bootstrapPending[netName]
	if !ok {
		return
	}
	delete(pending, nsName)
	if len(pending) == 0 {
		delete(c.bootstrapPending, netName)
	}
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

func (c *NamespaceController) namespaceHasAnyNetwork(nsName string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return len(c.nsNetworks[nsName]) > 0
}

// namespaceHasNetwork reports whether the last successfully-applied state
// for nsName under netName was active. Mirrors NodeController.nodeHasNetwork.
// Used by the per-reconcile transition gate to detect membership changes
// (a namespace moving between handlers via a NAD change).
func (c *NamespaceController) namespaceHasNetwork(netName, nsName string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	nss := c.nsActive[netName]
	if len(nss) == 0 {
		return false
	}
	_, ok := nss[nsName]
	return ok
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
		// Bare key, no separator — this is an informer-driven event for
		// the namespace resource itself (network unscoped).
		return key, ""
	}
	// Presence of the separator means this is a scoped key. Roundtrip
	// it as-is even if one part is empty; otherwise
	// scopedNamespaceQueueKey("", "net") → "|net" would parse back to
	// nsName="|net" + netName="" and fan-out would loop, re-encoding
	// as "|net|net", "|net|net|net", etc. The reconcile entry point
	// short-circuits on empty nsName.
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
