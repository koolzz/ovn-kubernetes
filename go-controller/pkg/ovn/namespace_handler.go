// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	corev1 "k8s.io/api/core/v1"

	nscontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controllers/namespace"
)

// Compile-time assertion: DefaultNetworkController satisfies the
// shared NamespaceHandler interface. The controller's run() registers
// this implementation with the shared NamespaceController; the legacy
// retryNamespaces watch was removed in Phase 4.1.
var _ nscontroller.NamespaceHandler = (*DefaultNetworkController)(nil)

// ReconcileNamespace implements NamespaceHandler. The level-driven shape
// is mapped onto the existing snapshot-driven entry points for this
// substep — the parsed NamespaceAnnotationState is intentionally
// ignored, and add/update/delete are dispatched per the legacy methods
// which still read raw annotations from the namespace object.
//
// Future substeps will collapse the legacy methods into a single
// state-driven body that reads annotations through the
// NamespaceAnnotationState accessors, making the
// namespace-controller's parse-cache observable here. For now, this is
// a thin shim so the controller can satisfy the interface without
// reshaping every downstream call site.
func (oc *DefaultNetworkController) ReconcileNamespace(oldNS, newNS *corev1.Namespace, _, _ *nscontroller.NamespaceAnnotationState) error {
	switch {
	case newNS == nil:
		// Delete pass. oldNS is the best-available prior namespace
		// object; legacy deleteNamespace only reads the namespace's
		// name and the cached nsInfo, so a name-only stub is fine
		// when no real cached object is available.
		if oldNS == nil {
			return nil
		}
		return oc.deleteNamespace(oldNS)
	case oldNS == nil:
		return oc.AddNamespace(newNS)
	default:
		return oc.updateNamespace(oldNS, newNS)
	}
}

// SyncNamespaces implements NamespaceHandler. The legacy
// syncNamespaces signature takes []interface{} for retry-framework
// reasons; convert to that shape on the way in.
func (oc *DefaultNetworkController) SyncNamespaces(namespaces []*corev1.Namespace) error {
	objs := make([]interface{}, 0, len(namespaces))
	for _, ns := range namespaces {
		objs = append(objs, ns)
	}
	return oc.syncNamespaces(objs)
}
