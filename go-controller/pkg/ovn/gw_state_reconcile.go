// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// reconcileGWStateForNamespace is the namespace-driven entry point for
// gateway-route convergence. It recomputes desired state from
// (annotation + gatewayPodIndex), diffs against the applied snapshot,
// and applies the delta via the existing add/delete primitives. The
// snapshot is updated only on successful apply, so a partial failure
// leaves the next reconcile to re-converge.
//
// Currently dormant — no production caller. Phase 1b.6.c.2 wires it
// into addNamespace / updateNamespace / deleteNamespace; Phase 1b.6.c.3
// drops the direct programming on the gateway-pod paths in favor of an
// enqueue.
func (oc *DefaultNetworkController) reconcileGWStateForNamespace(ns string) error {
	desired, err := oc.computeDesiredGWStateForNamespace(ns)
	if err != nil {
		return err
	}
	applied := oc.nsAppliedGWState.Get(ns)
	delta := computeGWStateDelta(applied, desired)
	if delta.empty() {
		return nil
	}
	if err := applyGWStateDelta(ns, delta, oc); err != nil {
		return err
	}
	if desired.size() == 0 {
		oc.nsAppliedGWState.Delete(ns)
	} else {
		oc.nsAppliedGWState.Set(ns, desired)
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
