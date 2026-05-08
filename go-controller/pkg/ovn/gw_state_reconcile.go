// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	libovsdbops "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// reconcileGWStateForNamespace is the namespace-driven entry point for
// gateway-route convergence and the side effects that travel with it
// (external-gw-pod-ips annotation patch in single-zone modes,
// conntrack flush in multi-zone IC). It recomputes desired state from
// (annotation + gatewayPodIndex), diffs against the applied snapshot,
// applies the delta via the existing add/delete primitives, then runs
// the side effects so an operator-visible state change always lands
// together with its OVN route reconvergence. The snapshot is updated
// only on a successful apply; a partial failure leaves the next
// reconcile to re-converge and the side-effect retry to fire on that
// pass.
func (oc *DefaultNetworkController) reconcileGWStateForNamespace(ns string) error {
	desired, err := oc.computeDesiredGWStateForNamespace(ns)
	if err != nil {
		return err
	}
	applied := oc.nsAppliedGWState.Get(ns)
	return runGWReconcile(ns, applied, desired, oc, oc.applyGWStateSideEffects, oc.nsAppliedGWState)
}

// runGWReconcile is the orchestration core of reconcileGWStateForNamespace.
// Computes the delta, applies it if non-empty, ALWAYS runs side
// effects, and updates the applied snapshot. Extracted from the
// method so the "side effects always run" contract is unit-testable
// without standing up the full DefaultNetworkController.
//
// Why side effects run unconditionally:
//   - Bootstrap: bootstrapNSAppliedGWState seeds the applied snapshot
//     from NBDB routes only. If NBDB matches desired but the
//     external-gw-pod-ips annotation is stale (controller crashed
//     mid-patch, manual kubectl edit, etc.), the delta is empty and
//     gating side effects on delta would never fix the annotation.
//   - Drift: even after bootstrap, if the annotation is externally
//     mutated, the only path that re-publishes it is applyGWStateSideEffects.
//
// Cost of the always-run:
//   - Single-zone: UpdateExternalGatewayPodIPsAnnotation is idempotent
//     at the apiserver — a patch with the current value is a no-op.
//   - Multi-zone IC: syncConntrackForExternalGateways is a deliberate
//     cleanup pass that walks pods, resolves MACs, and issues per-IP
//     conntrack deletes. Cheap when the gateway-IP set is stable
//     (nothing matches the wrong-criteria predicate) but not free;
//     this is an explicit "always reconcile conntrack" decision.
func runGWReconcile(
	ns string,
	applied, desired *desiredGWState,
	programmer gwRouteProgrammer,
	sideEffects func(ns string, desired *desiredGWState) error,
	snapshot *nsAppliedGWState,
) error {
	delta := computeGWStateDelta(applied, desired)
	if !delta.empty() {
		if err := applyGWStateDelta(ns, delta, programmer); err != nil {
			return err
		}
	}
	if err := sideEffects(ns, desired); err != nil {
		klog.Errorf("Gateway-state side effects for namespace %s failed: %v", ns, err)
	}
	if desired.size() == 0 {
		snapshot.Delete(ns)
	} else {
		snapshot.Set(ns, desired)
	}
	return nil
}

// applyGWStateSideEffects performs the operator-visible side effects
// that travel with gateway-state convergence. Two distinct sets per
// IC mode (matching the legacy gateway-pod path):
//
//   - Single-zone (or non-IC): patch the namespace
//     k8s.ovn.org/external-gw-pod-ips annotation with the pod-derived
//     gateway IPs only. ovnkube-node uses this hint to flush conntrack.
//   - Multi-zone IC: flush conntrack directly with the full
//     (annotation + pod + APB) union; no annotation hint is published
//     because ovnkube-controller owns the flush in this mode.
func (oc *DefaultNetworkController) applyGWStateSideEffects(ns string, desired *desiredGWState) error {
	if !config.OVNKubernetesFeature.EnableInterconnect || oc.zone == types.OvnDefaultZone {
		// Single-zone (or non-IC): annotation patch with pod-derived
		// gateway IPs only.
		podGWSet := sets.New[string]()
		if oc.gatewayPodIndex != nil {
			for ip := range oc.gatewayPodIndex.GatewaysForNamespace(ns) {
				podGWSet.Insert(ip)
			}
		}
		if err := util.UpdateExternalGatewayPodIPsAnnotation(oc.kube, ns, sets.List(podGWSet)); err != nil {
			klog.Errorf("Unable to update %s annotation for namespace %s: %v",
				util.ExternalGatewayPodIPsAnnotation, ns, err)
		}
		return nil
	}

	// Multi-zone IC: flush conntrack with the full union of (APB +
	// desired). desired is already (annotation + gatewayPodIndex)
	// merged.
	gatewayIPs, err := oc.apbExternalRouteController.GetAdminPolicyBasedExternalRouteIPsForTargetNamespace(ns)
	if err != nil {
		return fmt.Errorf("unable to retrieve gateway IPs for APB external route objects: %w", err)
	}
	if desired != nil {
		gatewayIPs.Insert(desired.ipSet()...)
	}
	if err := oc.syncConntrackForExternalGateways(ns, gatewayIPs); err != nil {
		klog.Errorf("Conntrack flush for namespace %s failed: %v", ns, err)
	}
	return nil
}

// computeDesiredGWStateForNamespace builds the per-namespace desired
// gateway state by merging the annotation-derived and pod-derived
// sources. A namespace that's gone from the informer (deletion in
// flight) has no annotation contribution; the gateway-pod index still
// contributes if any active gateway pod is targeting the name.
func (oc *DefaultNetworkController) computeDesiredGWStateForNamespace(ns string) (*desiredGWState, error) {
	nsObj, err := oc.watchFactory.GetNamespace(ns)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	var annotation gatewayInfo
	if nsObj != nil {
		annotation = parseAnnotationGWs(nsObj)
	}
	var podGWs map[string]bool
	if oc.gatewayPodIndex != nil {
		podGWs = oc.gatewayPodIndex.GatewaysForNamespace(ns)
	}
	return computeDesiredGWState(annotation, podGWs), nil
}

// parseAnnotationGWs returns the gateway-info derived from a
// namespace's routing-external-gws and bfd annotations. Returns the
// zero value (gws == nil) when no routing annotation is present or the
// annotation fails to parse — a malformed annotation is logged
// elsewhere by the caller; here we treat it as "no annotation gws".
func parseAnnotationGWs(ns *corev1.Namespace) gatewayInfo {
	annotation, ok := ns.Annotations[util.RoutingExternalGWsAnnotation]
	if !ok || annotation == "" {
		return gatewayInfo{}
	}
	gws, err := util.ParseRoutingExternalGWAnnotation(annotation)
	if err != nil {
		return gatewayInfo{}
	}
	_, bfdEnabled := ns.Annotations[util.BfdAnnotation]
	return gatewayInfo{gws: gws, bfdEnabled: bfdEnabled}
}

// addRoutesForNamespace satisfies the gwRouteProgrammer interface.
// Adapter to the existing addGWRoutesForNamespace primitive.
func (oc *DefaultNetworkController) addRoutesForNamespace(ns string, info gatewayInfo) error {
	return oc.addGWRoutesForNamespace(ns, info)
}

// deleteRoutesForNamespace satisfies the gwRouteProgrammer interface.
// Adapter to the existing deleteGWRoutesForNamespace primitive.
func (oc *DefaultNetworkController) deleteRoutesForNamespace(ns string, matchGWs sets.Set[string]) error {
	return oc.deleteGWRoutesForNamespace(ns, matchGWs)
}

// bootstrapNSAppliedGWState seeds the per-namespace applied-state
// snapshot AND the externalGatewayRouteInfo cache from NBDB at
// controller startup. Both caches are required for a restart-safe
// delete leg:
//
//   - nsAppliedGWState[ns] tells the namespace reconcile what routes
//     are currently programmed, so the diff against desired state
//     produces a correct delete delta when an annotation was cleared
//     during downtime.
//   - externalGatewayRouteInfo[podKey] is what deleteGWRoutesForNamespace
//     actually walks. Without it, the delete delta computed above would
//     fire but the cache walk would visit no pods, leaving the stale
//     NBDB routes in place permanently (the cache only repopulates on
//     pod-add, and pod-add no longer fires for already-running pods).
//
// Walks every gateway-pod-style logical-router static-route the
// addGWRoutesForPod primitive creates (policy=src-ip,
// ecmp_symmetric_reply=true, OutputPort matching the rtoe- prefix),
// resolves each route's pod IP to (namespace, pod name) via the
// informer, and seeds both caches with (gw IP, BFD-enabled, GR name).
// Routes whose pod IP is no longer in the informer are skipped —
// they're orphans that the per-namespace reconcile will cover once a
// real namespace event fires for any name they belong to.
func (oc *DefaultNetworkController) bootstrapNSAppliedGWState() error {
	if oc.nsAppliedGWState == nil {
		oc.nsAppliedGWState = newNSAppliedGWState()
	}
	pods, err := oc.watchFactory.GetAllPods()
	if err != nil {
		return fmt.Errorf("failed to list pods for nsAppliedGWState bootstrap: %w", err)
	}
	// Build podIP → (namespace, pod) so we can seed both caches in
	// one pass. The previous version only kept the namespace, which
	// was enough for nsAppliedGWState but left externalGatewayRouteInfo
	// unseeded — its key is the pod's NamespacedName.
	podIPToPod := map[string]ktypes.NamespacedName{}
	for _, pod := range pods {
		if pod.Spec.HostNetwork {
			continue
		}
		key := ktypes.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		for _, podIP := range pod.Status.PodIPs {
			podIPToPod[podIP.IP] = key
		}
	}

	predicate := func(item *nbdb.LogicalRouterStaticRoute) bool {
		if item.Policy == nil || *item.Policy != nbdb.LogicalRouterStaticRoutePolicySrcIP {
			return false
		}
		if item.Options["ecmp_symmetric_reply"] != "true" {
			return false
		}
		if item.OutputPort == nil {
			return false
		}
		return strings.Contains(*item.OutputPort, types.GWRouterToExtSwitchPrefix)
	}
	routes, err := libovsdbops.FindLogicalRouterStaticRoutesWithPredicate(oc.nbClient, predicate)
	if err != nil {
		return fmt.Errorf("failed to find gateway-pod static routes for bootstrap: %w", err)
	}

	seedGWStateFromRoutes(routes, podIPToPod, oc.nsAppliedGWState, oc.externalGatewayRouteInfo)
	return nil
}

// seedGWStateFromRoutes is the pure-function core of
// bootstrapNSAppliedGWState. Given a list of gateway-pod static routes
// and the current podIP → (namespace, pod) map, populates the
// applied-state snapshot (per namespace) and the gateway-route cache
// (per pod). Both must be seeded together: nsAppliedGWState drives the
// delete diff, externalGatewayRouteInfo drives the actual NBDB cleanup
// walk. Seeding one without the other leaves the cleanup half-done
// after a restart.
//
// Extracted from the bootstrap method so it's unit-testable without a
// real nbClient or watchFactory.
func seedGWStateFromRoutes(
	routes []*nbdb.LogicalRouterStaticRoute,
	podIPToPod map[string]ktypes.NamespacedName,
	applied *nsAppliedGWState,
	routeCache *apbroutecontroller.ExternalGatewayRouteInfoCache,
) {
	perNS := map[string]*desiredGWState{}
	orphans := 0
	for _, route := range routes {
		podIP := route.IPPrefix
		if idx := strings.IndexByte(podIP, '/'); idx > 0 {
			podIP = podIP[:idx]
		}
		podKey, ok := podIPToPod[podIP]
		if !ok {
			orphans++
			continue
		}
		ns := podKey.Namespace

		if perNS[ns] == nil {
			perNS[ns] = newDesiredGWState()
		}
		bfd := route.BFD != nil && *route.BFD != ""
		perNS[ns].addGW(route.Nexthop, bfd)

		if routeCache == nil || route.OutputPort == nil {
			continue
		}
		gr := grNameFromOutputPort(*route.OutputPort)
		if gr == "" {
			continue
		}
		_ = routeCache.CreateOrLoad(podKey, func(routeInfo *apbroutecontroller.RouteInfo) error {
			if routeInfo.PodExternalRoutes[podIP] == nil {
				routeInfo.PodExternalRoutes[podIP] = map[string]string{}
			}
			routeInfo.PodExternalRoutes[podIP][route.Nexthop] = gr
			return nil
		})
	}

	if applied != nil {
		for ns, state := range perNS {
			applied.Set(ns, state)
		}
	}
	klog.Infof("Bootstrapped nsAppliedGWState from NBDB: %d namespaces seeded, %d total routes, %d orphans",
		len(perNS), len(routes), orphans)
}

// grNameFromOutputPort extracts the gateway-router name from a static
// route's OutputPort, shaped "<portPrefix>rtoe-<grName>". Returns ""
// if the rtoe- segment isn't present (the bootstrap predicate already
// guarantees its presence; the empty return is defensive against
// future predicate changes).
func grNameFromOutputPort(outputPort string) string {
	idx := strings.LastIndex(outputPort, types.GWRouterToExtSwitchPrefix)
	if idx < 0 {
		return ""
	}
	return outputPort[idx+len(types.GWRouterToExtSwitchPrefix):]
}
