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
// Networks. The NamespaceAnnotationState is intentionally unused for
// now — legacy add/update/delete still read raw annotations off the
// namespace object. shouldFilterNamespace mirrors the FilterOutResource
// gate that the per-UDN retry framework applies today, so namespaces
// not served by this network are skipped.
func (oc *BaseUserDefinedNetworkController) ReconcileNamespace(oldNS, newNS *corev1.Namespace, _, _ *nscontroller.NamespaceAnnotationState) error {
	var nsName string
	switch {
	case newNS == nil && oldNS == nil:
		return nil
	case newNS == nil:
		nsName = oldNS.Name
	default:
		nsName = newNS.Name
	}
	if oc.shouldFilterNamespace(nsName) {
		return nil
	}
	switch {
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
