// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/libovsdb/ovsdb"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	libovsdbops "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/errors"
)

func (oc *DefaultNetworkController) getRoutingExternalGWs(nsInfo *namespaceInfo) *gatewayInfo {
	res := gatewayInfo{}
	// return a copy of the object so it can be handled without the
	// namespace locked
	res.bfdEnabled = nsInfo.routingExternalGWs.bfdEnabled
	res.gws = sets.New(nsInfo.routingExternalGWs.gws.UnsortedList()...)
	return &res
}

// addLocalPodToNamespace returns pod's routing gateway info and the ops needed
// to add pod's IP to the namespace's address set and port group.
//
// The per-gateway-pod map is read from gatewayPodIndex (the new source of
// truth populated by Phase 1b shadow-writes). The annotation-derived
// gateway info is still read from nsInfo until later substeps.
func (oc *DefaultNetworkController) addLocalPodToNamespace(ns string, portUUID string) (*gatewayInfo, map[string]gatewayInfo, []ovsdb.Operation, error) {
	var err error
	nsInfo, nsUnlock, err := oc.ensureNamespaceLocked(ns, true, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to ensure namespace locked: %v", err)
	}

	defer nsUnlock()

	ops, err := oc.addLocalPodToNamespaceLocked(nsInfo, portUUID)
	if err != nil {
		return nil, nil, nil, err
	}
	var podGWs map[string]gatewayInfo
	if oc.gatewayPodIndex != nil {
		podGWs = oc.gatewayPodIndex.PerPodGatewaysForNamespace(ns)
	} else {
		podGWs = map[string]gatewayInfo{}
	}
	return oc.getRoutingExternalGWs(nsInfo), podGWs, ops, nil
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
		_, bfdEnabled := ns.Annotations[util.BfdAnnotation]
		exGateways, err := util.ParseRoutingExternalGWAnnotation(annotation)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to parse external gateway annotation (%v)", err))
			// Match legacy behavior on a parse failure: leave the
			// annotation gateways unset but still propagate the BFD
			// flag for any reader that consults it without the gw set.
			nsInfo.routingExternalGWs.bfdEnabled = bfdEnabled
		} else {
			// Keep nsInfo.routingExternalGWs in sync for the legacy
			// cross-readers (addLocalPodToNamespace,
			// addPodExternalGWForNamespace conntrack-merge,
			// checkAndDeleteStaleConntrackEntries). Route programming
			// itself is driven by reconcileGWStateForNamespace below.
			nsInfo.routingExternalGWs = gatewayInfo{gws: exGateways, bfdEnabled: bfdEnabled}
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
		// if old gw annotation was empty, new one must not be empty, so we should remove any per pod SNAT towards nodeIP
		if oldGWAnnotation == "" {
			if config.Gateway.DisableSNATMultipleGWs {
				existingPods, err := oc.watchFactory.GetPods(old.Name)
				if err != nil {
					errors = append(errors, fmt.Errorf("failed to get all the pods (%v)", err))
				}
				for _, pod := range existingPods {
					if !oc.isPodScheduledinLocalZone(pod) {
						continue
					}

					logicalPort := util.GetLogicalPortName(pod.Namespace, pod.Name)
					if util.PodWantsHostNetwork(pod) {
						continue
					}
					podIPs, err := util.GetPodIPsOfNetwork(pod, oc.GetNetInfo(), nil)
					if err != nil {
						errors = append(errors, fmt.Errorf("unable to get pod %q IPs for SNAT rule removal err (%v)", logicalPort, err))
					}
					ips := make([]*net.IPNet, 0, len(podIPs))
					for _, podIP := range podIPs {
						ips = append(ips, &net.IPNet{IP: podIP})
					}
					if len(ips) > 0 {
						if extIPs, err := getExternalIPsGR(oc.watchFactory, pod.Spec.NodeName); err != nil {
							errors = append(errors, err)
						} else if err = oc.deletePodSNAT(pod.Spec.NodeName, extIPs, ips); err != nil {
							errors = append(errors, err)
						}
					}
				}
			}
		}
		// Update nsInfo.routingExternalGWs to reflect the new annotation
		// state for legacy cross-readers (conntrack merge, pod-add path).
		// The actual route convergence is driven by
		// reconcileGWStateForNamespace, which diffs (annotation +
		// gatewayPodIndex) desired against the applied snapshot and
		// emits exactly the deltas required.
		if gwAnnotation == "" {
			nsInfo.routingExternalGWs = gatewayInfo{}
		} else if exGateways, err := util.ParseRoutingExternalGWAnnotation(gwAnnotation); err == nil {
			nsInfo.routingExternalGWs = gatewayInfo{gws: exGateways, bfdEnabled: newBFDEnabled}
		} else {
			errors = append(errors, err)
		}
		if err := oc.reconcileGWStateForNamespace(old.Name); err != nil {
			errors = append(errors, fmt.Errorf("failed to apply gateway state for namespace %s: %v", old.Name, err))
		}
		if config.OVNKubernetesFeature.EnableInterconnect && oc.zone != types.OvnDefaultZone {
			// If interconnect is disabled OR interconnect is running in single-zone-mode,
			// the ovnkube-master is responsible for patching ICNI managed namespaces with
			// "k8s.ovn.org/external-gw-pod-ips". In that case, we need ovnkube-node to flush
			// conntrack on every node. In multi-zone-interconnect case, we will handle the flushing
			// directly on the ovnkube-controller code to avoid an extra namespace annotation
			gatewayIPs, err := oc.apbExternalRouteController.GetAdminPolicyBasedExternalRouteIPsForTargetNamespace(old.Name)
			if err != nil {
				return fmt.Errorf("unable to retrieve gateway IPs for Admin Policy Based External Route objects for namespace %s: %w", old.Name, err)
			}
			// Gateway-pod GWs come from gatewayPodIndex (Phase 1b
			// source of truth). Annotation-derived ns GWs still live
			// on nsInfo until later substeps.
			if oc.gatewayPodIndex != nil {
				for ip := range oc.gatewayPodIndex.GatewaysForNamespace(old.Name) {
					gatewayIPs.Insert(ip)
				}
			}
			gatewayIPs.Insert(nsInfo.routingExternalGWs.gws.UnsortedList()...)
			err = oc.syncConntrackForExternalGateways(old.Name, gatewayIPs) // best effort
			if err != nil {
				klog.Errorf("Syncing conntrack entries for egressGWs %+v serving the namespace %s failed: %v",
					gatewayIPs, old.Name, err)
			}
		}
		// if new annotation is empty, exgws were removed, may need to add SNAT per pod
		// check if there are any pod gateways serving this namespace as well
		hasPodGWs := false
		if oc.gatewayPodIndex != nil {
			hasPodGWs = len(oc.gatewayPodIndex.PodsForNamespace(old.Name)) > 0
		}
		if gwAnnotation == "" && !hasPodGWs && config.Gateway.DisableSNATMultipleGWs {
			existingPods, err := oc.watchFactory.GetPods(old.Name)
			if err != nil {
				errors = append(errors, fmt.Errorf("failed to get all the pods (%v)", err))
			}
			for _, pod := range existingPods {
				if !oc.isPodScheduledinLocalZone(pod) && !util.PodNeedsSNAT(pod) {
					continue
				}
				podAnnotation, err := util.UnmarshalPodAnnotation(pod.Annotations, types.DefaultNetworkName)
				if err != nil {
					errors = append(errors, err)
				} else {
					// Helper function to handle the complex SNAT operations
					handleSNATOps := func() error {
						ops, err := oc.AddPodSNATOps(pod.Spec.NodeName, podAnnotation.IPs)
						if err != nil {
							return err
						}

						// Execute all operations in a single transaction
						if len(ops) > 0 {
							_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
							if err != nil {
								return fmt.Errorf("failed to update SNAT for pod %s on router %s: %v", pod.Name, oc.GetNetworkScopedGWRouterName(pod.Spec.NodeName), err)
							}
						}
						return nil
					}

					if err := handleSNATOps(); err != nil {
						errors = append(errors, err)
					}
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
