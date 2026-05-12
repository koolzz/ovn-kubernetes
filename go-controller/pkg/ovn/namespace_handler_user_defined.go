// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	corev1 "k8s.io/api/core/v1"

	nscontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controllers/namespace"
)

// Compile-time assertions: every concrete UDN controller satisfies the
// shared NamespaceHandler interface through methods on
// *BaseUserDefinedNetworkController. Each UDN's run() registers itself
// with the shared NamespaceController via registerNamespaceReconciler;
// the legacy per-network retryNamespaces watch was removed in Phase 4.1.
var (
	_ nscontroller.NamespaceHandler = (*Layer3UserDefinedNetworkController)(nil)
	_ nscontroller.NamespaceHandler = (*Layer2UserDefinedNetworkController)(nil)
	_ nscontroller.NamespaceHandler = (*LocalnetUserDefinedNetworkController)(nil)
)

// ReconcileNamespace implements NamespaceHandler for User Defined
// Networks. Pure applier: the shared NamespaceController's transition
// gate (ClaimsNamespace + namespaceHasNetwork) decides membership
// before dispatch, so this handler trusts the (oldNS, newNS) it
// receives:
//
//   - (oldNS, newNS) update with newNS != nil → handler claims now.
//   - (nil,   newNS) fresh add → handler claims now.
//   - (oldNS, nil)   delete → handler had applied state. May or may
//     not still claim; programming the delete unconditionally is the
//     load-bearing invariant. If we re-checked current membership
//     here, an active→inactive NAD transition would short-circuit
//     and leave OVN state for the previous owner orphaned (the
//     namespace-controller would clear active+cache and never call
//     the handler again).
//
// NamespaceAnnotationState is intentionally unused — legacy
// add/update/delete still read raw annotations off the namespace
// object.
func (oc *BaseUserDefinedNetworkController) ReconcileNamespace(oldNS, newNS *corev1.Namespace, _, _ *nscontroller.NamespaceAnnotationState) error {
	switch {
	case newNS == nil && oldNS == nil:
		return nil
	case newNS == nil:
		return oc.deleteNamespaceForUserDefinedNetwork(oldNS)
	case oldNS == nil:
		return oc.AddNamespaceForUserDefinedNetwork(newNS)
	default:
		return oc.updateNamespaceForUserDefinedNetwork(oldNS, newNS)
	}
}

// SyncNamespaces implements NamespaceHandler. The legacy syncNamespaces
// (on BaseNetworkController, shared with the default network) takes
// []interface{} for retry-framework reasons; convert on the way in.
// Sync is called with every namespace in the cluster — not just those
// served by this network — because the cleanup it performs is keyed by
// controllerName, which scopes effects to this UDN regardless.
func (oc *BaseUserDefinedNetworkController) SyncNamespaces(namespaces []*corev1.Namespace) error {
	objs := make([]interface{}, 0, len(namespaces))
	for _, ns := range namespaces {
		objs = append(objs, ns)
	}
	return oc.syncNamespaces(objs)
}
