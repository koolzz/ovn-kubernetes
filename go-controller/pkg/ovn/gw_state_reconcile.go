// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
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
