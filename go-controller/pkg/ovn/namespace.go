// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/libovsdb/ovsdb"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/errors"
)

// addLocalPodToNamespace returns pod's routing gateway info and the ops needed
// to add pod's IP to the namespace's address set and port group.
//
// Both gateway sources are read from their post-migration owners:
//   - per-gateway-pod entries from gatewayPodIndex.
//   - annotation-derived entries parsed directly from the namespace
//     informer object (parseAnnotationGWs uses the same parser the
//     apply primitive uses, so the pod-add path and namespace reconcile
//     see the same desired state for any single namespace event).
func (oc *DefaultNetworkController) addLocalPodToNamespace(ns string, portUUID string) (*gatewayInfo, map[string]gatewayInfo, []ovsdb.Operation, error) {
	nsInfo, nsUnlock, err := oc.ensureNamespaceLocked(ns, true, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to ensure namespace locked: %v", err)
	}
	defer nsUnlock()

	ops, err := oc.addLocalPodToNamespaceLocked(nsInfo, portUUID)
	if err != nil {
		return nil, nil, nil, err
	}

	annoGWs := &gatewayInfo{gws: sets.New[string]()}
	if nsObj, err := oc.watchFactory.GetNamespace(ns); err == nil && nsObj != nil {
		parsed := parseAnnotationGWs(nsObj)
		if parsed.gws != nil {
			annoGWs.gws = sets.New(parsed.gws.UnsortedList()...)
		}
		annoGWs.bfdEnabled = parsed.bfdEnabled
	}
	var podGWs map[string]gatewayInfo
	if oc.gatewayPodIndex != nil {
		podGWs = oc.gatewayPodIndex.PerPodGatewaysForNamespace(ns)
	} else {
		podGWs = map[string]gatewayInfo{}
	}
	return annoGWs, podGWs, ops, nil
}

func isNamespaceMulticastEnabled(annotations map[string]string) bool {
	return annotations[util.NsMulticastAnnotation] == "true"
}

// AddNamespace creates corresponding addressset in ovn db
func (oc *DefaultNetworkController) AddNamespace(ns *corev1.Namespace) error {
	klog.Infof("[%s] adding namespace", ns.Name)
	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s] adding namespace took %v", ns.Name, time.Since(start))
	}()

	_, nsUnlock, err := oc.ensureNamespaceLocked(ns.Name, false, ns)
	if err != nil {
		return fmt.Errorf("failed to ensure namespace locked: %v", err)
	}
	defer nsUnlock()
	return nil
}

// configureNamespace ensures internal structures are updated based on namespace
// must be called with nsInfo lock
func (oc *DefaultNetworkController) configureNamespace(nsInfo *namespaceInfo, ns *corev1.Namespace) error {
	var errors []error

	if annotation, ok := ns.Annotations[util.RoutingExternalGWsAnnotation]; ok {
		// We still parse the annotation here so a malformed value is
		// surfaced through the handler's error return; the parsed
		// value itself is consumed by the apply primitive below, which
		// re-parses from the same namespace object.
		if _, err := util.ParseRoutingExternalGWAnnotation(annotation); err != nil {
			errors = append(errors, fmt.Errorf("failed to parse external gateway annotation (%v)", err))
		}
	}

	// Drive route programming through the namespace-level apply
	// primitive. It reads desired state from (annotation +
	// gatewayPodIndex), diffs against the applied snapshot, and
	// invokes the existing add/delete primitives idempotently.
	if err := oc.reconcileGWStateForNamespace(ns.Name); err != nil {
		errors = append(errors, fmt.Errorf("failed to apply gateway state for namespace %s: %v", ns.Name, err))
	}

	if err := oc.configureNamespaceCommon(nsInfo, ns); err != nil {
		errors = append(errors, err)
	}
	return utilerrors.Join(errors...)
}

func (oc *DefaultNetworkController) updateNamespace(old, newer *corev1.Namespace) error {
	var errors []error
	klog.Infof("[%s] updating namespace", old.Name)

	nsInfo, nsUnlock := oc.getNamespaceLocked(old.Name, false)
	if nsInfo == nil {
		klog.Warningf("Update event for unknown namespace %q", old.Name)
		return nil
	}
	defer nsUnlock()

	gwAnnotation := newer.Annotations[util.RoutingExternalGWsAnnotation]
	oldGWAnnotation := old.Annotations[util.RoutingExternalGWsAnnotation]
	_, newBFDEnabled := newer.Annotations[util.BfdAnnotation]
	_, oldBFDEnabled := old.Annotations[util.BfdAnnotation]

	if gwAnnotation != oldGWAnnotation || newBFDEnabled != oldBFDEnabled {
		// Surface a parse error to the caller; the apply primitive
		// below consumes the parsed annotation directly off the
		// namespace object, so no nsInfo write is required here.
		if gwAnnotation != "" {
			if _, err := util.ParseRoutingExternalGWAnnotation(gwAnnotation); err != nil {
				errors = append(errors, err)
			}
		}
		// reconcileGWStateForNamespace drives both the route deltas and
		// the IC-mode-specific side effects (annotation patch /
		// conntrack flush) — see applyGWStateSideEffects in
		// gw_state_reconcile.go. The "gateway-added → drop existing
		// per-pod SNAT" cleanup that used to live here is redundant
		// now: addGWRoutesForNamespace inside the apply primitive
		// already calls deletePodSNAT per pod (see
		// egressgw.go:addGWRoutesForNamespace) when DisableSNATMultipleGWs.
		if err := oc.reconcileGWStateForNamespace(old.Name); err != nil {
			errors = append(errors, fmt.Errorf("failed to apply gateway state for namespace %s: %v", old.Name, err))
		}
		// "Gateway-removed → restore per-pod SNAT": fan out per-pod
		// reconcile via the level-driven ReconcilePod entry point. The
		// pod controller's add path re-runs, sees no gateways for the
		// namespace, and programs SNAT through its own normal flow —
		// no inline SNAT op-construction in the namespace handler.
		// HasActiveGWPods (not PodsForNamespace) is the right gate
		// here: inactive payloads — pods kept in the index but not
		// ready or without resolved gateway IPs — must NOT prevent
		// the SNAT restore. Otherwise, removing the last namespace
		// annotation while only an inactive pod candidate remained
		// would leave per-pod SNAT torn down even though no active
		// external gateway is left.
		hasActiveGWPods := false
		if oc.gatewayPodIndex != nil {
			hasActiveGWPods = oc.gatewayPodIndex.HasActiveGWPods(old.Name)
		}
		if gwAnnotation == "" && !hasActiveGWPods && config.Gateway.DisableSNATMultipleGWs {
			existingPods, err := oc.watchFactory.GetPods(old.Name)
			if err != nil {
				errors = append(errors, fmt.Errorf("failed to list pods for SNAT fan-out: %v", err))
			}
			for _, pod := range existingPods {
				podKey := pod.Namespace + "/" + pod.Name
				if err := oc.ReconcilePod(podKey); err != nil {
					errors = append(errors, fmt.Errorf("failed to enqueue pod %s for SNAT reconcile: %v", podKey, err))
				}
			}
		}
	}
	aclAnnotation := newer.Annotations[util.AclLoggingAnnotation]
	oldACLAnnotation := old.Annotations[util.AclLoggingAnnotation]
	// support for ACL logging update, if new annotation is empty, make sure we propagate new setting
	if aclAnnotation != oldACLAnnotation {
		if err := oc.updateNamespaceAclLogging(old.Name, aclAnnotation, nsInfo); err != nil {
			errors = append(errors, err)
		}
		if oc.efController != nil {
			// Trigger an egress fw logging update - this will only happen if an egress firewall exists for the NS, otherwise
			// this will not do anything.
			egressFirewalls, err := oc.watchFactory.EgressFirewallInformer().Lister().EgressFirewalls(old.Name).List(labels.Everything())
			if err != nil {
				errors = append(errors, err)
			}
			for _, fw := range egressFirewalls {
				fwKey, err := cache.MetaNamespaceKeyFunc(fw)
				if err != nil {
					klog.Errorf("Failed to get key for EgressFirewall %s/%s, will not update ACL logging: %v", old.Name, fwKey, err)
					continue
				}
				klog.Infof("Namespace %s: EgressFirewall ACL logging setting updating to deny=%s allow=%s",
					old.Name, nsInfo.aclLogging.Deny, nsInfo.aclLogging.Allow)
				oc.efController.Reconcile(fwKey)
			}
		}
	}

	if err := oc.multicastUpdateNamespace(newer, nsInfo); err != nil {
		errors = append(errors, err)
	}
	return utilerrors.Join(errors...)
}

func (oc *DefaultNetworkController) deleteNamespace(ns *corev1.Namespace) error {
	klog.Infof("[%s] deleting namespace", ns.Name)

	nsInfo, err := oc.deleteNamespaceLocked(ns.Name)
	if err != nil {
		return err
	}
	if nsInfo == nil {
		return nil
	}
	defer nsInfo.Unlock()

	// Drive the gateway-route teardown through the apply primitive:
	// the namespace is gone from the informer so desired state is
	// empty (modulo any active gateway-pod IPs in the index, which
	// would still target this name), the diff against the applied
	// snapshot produces the right delete delta, and the snapshot is
	// cleared on success.
	if err := oc.reconcileGWStateForNamespace(ns.Name); err != nil {
		return fmt.Errorf("failed to apply gateway state teardown for namespace %s: %v", ns.Name, err)
	}
	if err := oc.multicastDeleteNamespace(ns, nsInfo); err != nil {
		return fmt.Errorf("failed to delete multicast namespace error %v", err)
	}
	return nil
}

// ensureNamespaceLocked locks namespacesMutex, gets/creates an entry for ns, configures OVN nsInfo, and returns it
// with its mutex locked.
// ns is the name of the namespace, while namespace is the optional k8s namespace object
func (oc *DefaultNetworkController) ensureNamespaceLocked(ns string, readOnly bool, namespace *corev1.Namespace) (*namespaceInfo, func(), error) {
	return oc.ensureNamespaceLockedCommon(ns, readOnly, namespace, oc.configureNamespace)
}
