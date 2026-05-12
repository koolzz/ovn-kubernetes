// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	v1pod "k8s.io/kubernetes/pkg/api/v1/pod"
	utilnet "k8s.io/utils/net"

	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"
	"github.com/ovn-kubernetes/libovsdb/ovsdb"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	libovsdbops "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	libovsdbutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/util"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/nbdb"
	apbroutecontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/ovn/controller/apbroute"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

type gatewayInfo struct {
	gws        sets.Set[string]
	bfdEnabled bool
}

// addPodExternalGW handles detecting if a pod is serving as an external gateway for namespace(s)
// and reconciling per-namespace gateway state. gatewayPodIndex is the sole source of truth and is
// updated at the top so the reconcile invocations below see the post-update view; reconcile
// recomputes desired state from (annotation + index) for each target namespace and drives both
// route programming and side effects.
func (oc *DefaultNetworkController) addPodExternalGW(pod *corev1.Pod) error {
	if oc.gatewayPodIndex != nil {
		_ = oc.gatewayPodIndex.Update(pod)
	}

	podRoutingNamespaceAnno := pod.Annotations[util.RoutingNamespaceAnnotation]
	if podRoutingNamespaceAnno == "" {
		return nil
	}

	klog.Infof("External gateway pod: %s, detected for namespace(s) %s", pod.Name, podRoutingNamespaceAnno)

	// If an external gateway pod is in terminating or not ready state then don't add the
	// routes for the external gateway pod.
	if util.PodTerminating(pod) || !v1pod.IsPodReadyConditionTrue(pod.Status) {
		klog.Warningf("External gateway pod cannot serve traffic; it's in terminating or not ready state: %s/%s", pod.Namespace, pod.Name)
		return nil
	}

	foundGws, err := getExGwPodIPs(pod)
	if err != nil {
		klog.Errorf("Error getting exgw IPs for pod: %s, error: %v", pod.Name, err)
		oc.recordPodEvent("ErrorAddingLogicalPort", err, pod)
		return nil
	}
	if foundGws.Len() == 0 {
		klog.Warningf("No valid gateway IPs found for requested external gateway pod: %s", pod.Name)
		return nil
	}

	for _, namespace := range strings.Split(podRoutingNamespaceAnno, ",") {
		if err := oc.reconcileGWStateForNamespace(namespace); err != nil {
			return fmt.Errorf("failed to reconcile GW state for pod %s/%s in namespace %s: %w",
				pod.Namespace, pod.Name, namespace, err)
		}
	}
	return nil
}

func (oc *DefaultNetworkController) syncConntrackForExternalGateways(namespace string, gwIPsToKeep sets.Set[string]) error {
	return util.SyncConntrackForExternalGateways(gwIPsToKeep, oc.isPodInLocalZone, func() ([]*corev1.Pod, error) {
		return oc.watchFactory.GetPods(namespace)
	})
}

func (oc *DefaultNetworkController) checkAndDeleteStaleConntrackEntries() {
	namespaces, err := oc.watchFactory.GetNamespaces()
	if err != nil {
		klog.Errorf("Unable to get pods from informer: %v", err)
		return
	}
	for _, namespace := range namespaces {
		// flush here since we know we have added an egressgw pod and we also know the full list of existing gatewayIPs
		existingGWs, err := oc.apbExternalRouteController.GetAdminPolicyBasedExternalRouteIPsForTargetNamespace(namespace.Name)
		if err != nil {
			klog.Errorf("Unable to retrieve gateway IPs for Admin Policy Based External Route objects for ns %s: %v", namespace.Name, err)
			return
		}
		// Gateway-pod-derived GWs come from gatewayPodIndex (Phase 1b
		// source of truth). Annotation-derived ns GWs come from the
		// namespace informer — parseAnnotationGWs is the same parser
		// the apply primitive uses, so this read agrees with what
		// reconcile would compute.
		if oc.gatewayPodIndex != nil {
			for ip := range oc.gatewayPodIndex.GatewaysForNamespace(namespace.Name) {
				existingGWs.Insert(ip)
			}
		}
		nsAnno := parseAnnotationGWs(namespace)
		existingGWs.Insert(nsAnno.gws.UnsortedList()...)
		if len(existingGWs) > 0 {
			pods, err := oc.watchFactory.GetPods(namespace.Name)
			if err != nil {
				klog.Warningf("Unable to get pods from informer for namespace %s: %v", namespace.Name, err)
			}
			if len(pods) > 0 || err != nil {
				// we only need to proceed if there is at least one pod in this namespace on this node
				// OR if we couldn't fetch the pods for some reason at this juncture
				err = oc.syncConntrackForExternalGateways(namespace.Name, existingGWs)
				if err != nil {
					klog.Errorf("Syncing conntrack entries for egressGWs %+v serving the namespace %s failed: %v",
						existingGWs, namespace.Name, err)
				}
			}
		}
	}
}

func (oc *DefaultNetworkController) isPodInLocalZone(pod *corev1.Pod) (bool, error) {
	node, err := oc.watchFactory.GetNode(pod.Spec.NodeName)
	if err != nil {
		return false, err
	}
	return oc.isLocalZoneNode(node), nil
}

// addGWRoutesForNamespace handles adding routes for all existing pods in namespace
func (oc *DefaultNetworkController) addGWRoutesForNamespace(namespace string, egress gatewayInfo) error {
	existingPods, err := oc.watchFactory.GetPods(namespace)
	if err != nil {
		return fmt.Errorf("failed to get all the pods (%v)", err)
	}
	for _, pod := range existingPods {
		if util.PodCompleted(pod) || util.PodWantsHostNetwork(pod) {
			continue
		}
		podIPs := make([]*net.IPNet, 0)
		for _, podIP := range pod.Status.PodIPs {
			podIP := &net.IPNet{IP: utilnet.ParseIPSloppy(podIP.IP)}
			podIP.Mask = util.GetIPFullMask(podIP.IP)
			podIPs = append(podIPs, podIP)
		}
		if len(podIPs) == 0 {
			klog.Warningf("Will not add gateway routes pod %s/%s. IPs not found!", pod.Namespace, pod.Name)
			continue
		}
		if config.Gateway.DisableSNATMultipleGWs {
			// delete all perPodSNATs (if this pod was controlled by egressIP controller, it will stop working since
			// a pod cannot be used for multiple-external-gateways and egressIPs at the same time)
			if err = oc.deletePodSNAT(pod.Spec.NodeName, []*net.IPNet{}, podIPs); err != nil {
				klog.Error(err.Error())
			}
		}
		podNsName := ktypes.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		if err := oc.addGWRoutesForPod([]*gatewayInfo{&egress}, podIPs, podNsName, pod.Spec.NodeName); err != nil {
			return err
		}
	}
	return nil
}

// createBFDStaticRouteOps appends the ops needed to create-or-update a
// gateway-pod-style logical-router static route into the provided ops
// slice. Separated from createBFDStaticRoute so callers building larger
// transactions (e.g. applyGWStateDelta's BFD-replace path) can batch
// add + delete legs into a single TransactAndCheck.
func (oc *DefaultNetworkController) createBFDStaticRouteOps(ops []ovsdb.Operation, bfdEnabled bool, gw, podIP, gr, port, mask string) ([]ovsdb.Operation, error) {
	lrsr := nbdb.LogicalRouterStaticRoute{
		Policy: &nbdb.LogicalRouterStaticRoutePolicySrcIP,
		Options: map[string]string{
			"ecmp_symmetric_reply": "true",
		},
		Nexthop:    gw,
		IPPrefix:   podIP + mask,
		OutputPort: &port,
	}
	var err error
	if bfdEnabled {
		bfd := nbdb.BFD{
			DstIP:       gw,
			LogicalPort: port,
		}
		ops, err = libovsdbops.CreateOrUpdateBFDOps(oc.nbClient, ops, &bfd)
		if err != nil {
			return nil, fmt.Errorf("error creating or updating BFD %+v: %v", bfd, err)
		}
		lrsr.BFD = &bfd.UUID
	}
	p := func(item *nbdb.LogicalRouterStaticRoute) bool {
		return item.IPPrefix == lrsr.IPPrefix &&
			item.Nexthop == lrsr.Nexthop &&
			item.OutputPort != nil &&
			*item.OutputPort == *lrsr.OutputPort &&
			item.Policy == lrsr.Policy
	}
	ops, err = libovsdbops.CreateOrUpdateLogicalRouterStaticRoutesWithPredicateOps(oc.nbClient, ops, gr, &lrsr, p,
		&lrsr.Options)
	if err != nil {
		return nil, fmt.Errorf("error creating or updating static route %+v on router %s: %v", lrsr, gr, err)
	}
	return ops, nil
}

func (oc *DefaultNetworkController) createBFDStaticRoute(bfdEnabled bool, gw string, podIP, gr, port, mask string) error {
	ops, err := oc.createBFDStaticRouteOps(nil, bfdEnabled, gw, podIP, gr, port, mask)
	if err != nil {
		return err
	}
	if _, err = libovsdbops.TransactAndCheck(oc.nbClient, ops); err != nil {
		return fmt.Errorf("error transacting static route: %v", err)
	}
	return nil
}

// deleteLogicalRouterStaticRouteOps appends the ops needed to delete
// every gateway-pod-style logical-router static route matching
// (policy=src-ip, ipPrefix=podIP/mask, nexthop=gw) on gr. Separated
// from deleteLogicalRouterStaticRoute so callers can batch.
func (oc *DefaultNetworkController) deleteLogicalRouterStaticRouteOps(ops []ovsdb.Operation, podIP, mask, gw, gr string) ([]ovsdb.Operation, error) {
	p := func(item *nbdb.LogicalRouterStaticRoute) bool {
		return item.Policy != nil &&
			*item.Policy == nbdb.LogicalRouterStaticRoutePolicySrcIP &&
			item.IPPrefix == podIP+mask &&
			item.Nexthop == gw
	}
	ops, err := libovsdbops.DeleteLogicalRouterStaticRoutesWithPredicateOps(oc.nbClient, ops, gr, p)
	if err != nil {
		return nil, fmt.Errorf("error building delete ops for static route on router %s: %v", gr, err)
	}
	return ops, nil
}

func (oc *DefaultNetworkController) deleteLogicalRouterStaticRoute(podIP, mask, gw, gr string) error {
	ops, err := oc.deleteLogicalRouterStaticRouteOps(nil, podIP, mask, gw, gr)
	if err != nil {
		return err
	}
	if _, err = libovsdbops.TransactAndCheck(oc.nbClient, ops); err != nil {
		return fmt.Errorf("error transacting static route delete: %v", err)
	}
	return nil
}

// deletePodGWRoute deletes all associated gateway routing resources for one
// pod gateway route
// this MUST be called with a lock on routeInfo
func (oc *DefaultNetworkController) deletePodGWRoute(routeInfo *apbroutecontroller.RouteInfo, podIP, gw, gr string) error {
	if utilnet.IsIPv6String(gw) != utilnet.IsIPv6String(podIP) {
		return nil
	}
	pod, err := oc.watchFactory.PodCoreInformer().Lister().Pods(routeInfo.PodName.Namespace).Get(routeInfo.PodName.Name)
	if err == nil {
		local, err := oc.isPodInLocalZone(pod)
		if err != nil {
			return err
		}
		if !local {
			klog.V(4).Infof("Not deleting exgw routes for pod %s not in the local zone %s", routeInfo.PodName, oc.zone)
			return nil
		}
	}
	mask := util.GetIPFullMaskString(podIP)
	if err := oc.deleteLogicalRouterStaticRoute(podIP, mask, gw, gr); err != nil {
		return fmt.Errorf("unable to delete pod %s ECMP route to GR %s, GW: %s: %w",
			routeInfo.PodName, gr, gw, err)
	}

	klog.V(5).Infof("ECMP route deleted for pod: %s, on gr: %s, to gw: %s",
		routeInfo.PodName, gr, gw)

	node := util.GetWorkerFromGatewayRouter(gr)

	// The gw is deleted from the routes cache after this func is called, length 1
	// means it is the last gw for the pod and the hybrid route policy should be deleted.
	if entry := routeInfo.PodExternalRoutes[podIP]; len(entry) <= 1 {
		if err := oc.delHybridRoutePolicyForPod(net.ParseIP(podIP), node); err != nil {
			return fmt.Errorf("unable to delete hybrid route policy for pod %s: err: %v", routeInfo.PodName, err)
		}
	}

	portPrefix, err := oc.extSwitchPrefix(node)
	if err != nil {
		return err
	}
	return oc.cleanUpBFDEntry(gw, gr, portPrefix)
}

// deletePodExternalGW detects if a given pod is acting as an external GW and reconciles per-namespace
// gateway state without it. gatewayPodIndex is the sole source of truth and is Delete'd at the top so
// reconcile sees the post-delete view; reconcile emits the right delete delta when no other source
// still desires this pod's gateway IPs.
//
// The reconcile set is the union of (a) the namespaces the index was
// previously serving for this pod and (b) the pod's current
// routing-namespaces annotation. Both sources are necessary because
// informer ordering doesn't guarantee the annotation is still present
// at delete time: if the annotation was cleared before the pod-delete
// event arrives, only the index has the historical targets, and
// skipping them would leak stale routes for namespaces that no longer
// appear in the pod object.
func (oc *DefaultNetworkController) deletePodExternalGW(pod *corev1.Pod) (err error) {
	var priorTargets sets.Set[string]
	if oc.gatewayPodIndex != nil {
		priorTargets = oc.gatewayPodIndex.Delete(makePodGWKey(pod))
	}
	targets := gatewayPodDeleteTargets(priorTargets, pod.Annotations[util.RoutingNamespaceAnnotation])
	if targets.Len() == 0 {
		return nil
	}
	klog.Infof("Deleting routes for external gateway pod: %s, for namespace(s) %v", pod.Name,
		sets.List(targets))
	for _, namespace := range sets.List(targets) {
		if err := oc.reconcileGWStateForNamespace(namespace); err != nil {
			// if we encounter error while reconciling one namespace we return and don't try subsequent namespaces
			return fmt.Errorf("failed to reconcile GW state for pod %s deletion in namespace %s: %w",
				pod.Name, namespace, err)
		}
	}
	return nil
}

// gatewayPodDeleteTargets is the pure-function core of
// deletePodExternalGW's reconcile-set computation. Given the namespaces
// the gatewayPodIndex was previously serving for the pod (returned by
// the Delete call) and the pod's current routing-namespaces annotation,
// returns the union — every namespace that needs a per-namespace
// reconcile to converge on the pod's absence.
//
// Both sources matter: informer ordering doesn't guarantee the
// annotation is still present at delete time. If the annotation was
// cleared in a prior update before the delete event arrives, only the
// index has the historical targets, and skipping them would leave
// stale OVN routes pointing at the now-deleted pod.
func gatewayPodDeleteTargets(priorTargets sets.Set[string], currentAnnotation string) sets.Set[string] {
	out := sets.New[string]()
	if priorTargets != nil {
		out = out.Union(priorTargets)
	}
	if currentAnnotation == "" {
		return out
	}
	for _, ns := range strings.Split(currentAnnotation, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		out.Insert(ns)
	}
	return out
}

// deleteGwRoutesForNamespace handles deleting routes to gateways for a pod on a specific GR.
// If a set of gateways is given, only routes for that gateway are deleted. If no gateways
// are given, all routes for the namespace are deleted.
func (oc *DefaultNetworkController) deleteGWRoutesForNamespace(namespace string, matchGWs sets.Set[string]) error {
	deleteAll := (matchGWs == nil || matchGWs.Len() == 0)

	policyGWIPs, err := oc.apbExternalRouteController.GetDynamicGatewayIPsForTargetNamespace(namespace)
	if err != nil {
		return err
	}
	policyStaticGWIPs, err := oc.apbExternalRouteController.GetStaticGatewayIPsForTargetNamespace(namespace)
	if err != nil {
		return err
	}
	policyGWIPs = policyGWIPs.Union(policyStaticGWIPs)
	return oc.externalGatewayRouteInfo.CleanupNamespace(namespace, func(routeInfo *apbroutecontroller.RouteInfo) error {
		for podIP, routes := range routeInfo.PodExternalRoutes {
			for gw, gr := range routes {
				if (deleteAll || matchGWs.Has(gw)) && !policyGWIPs.Has(gw) {
					if err := oc.deletePodGWRoute(routeInfo, podIP, gw, gr); err != nil {
						// if we encounter error while deleting routes for one pod; we return and don't try subsequent pods
						return fmt.Errorf("delete pod GW route failed: %w", err)
					}
					delete(routes, gw)
				}
			}
		}
		return nil
	})
}

// deleteGwRoutesForPod handles deleting all routes to gateways for a pod IP on a specific GR
func (oc *DefaultNetworkController) deleteGWRoutesForPod(name ktypes.NamespacedName, podIPNets []*net.IPNet) (err error) {
	return oc.externalGatewayRouteInfo.Cleanup(name, func(routeInfo *apbroutecontroller.RouteInfo) error {
		policyGWIPs, err := oc.apbExternalRouteController.GetDynamicGatewayIPsForTargetNamespace(name.Namespace)
		if err != nil {
			return err
		}
		policyStaticGWIPs, err := oc.apbExternalRouteController.GetStaticGatewayIPsForTargetNamespace(name.Namespace)
		if err != nil {
			return err
		}
		policyGWIPs = policyGWIPs.Union(policyStaticGWIPs)

		for _, podIPNet := range podIPNets {
			podIP := podIPNet.IP.String()
			routes, ok := routeInfo.PodExternalRoutes[podIP]
			if !ok {
				continue
			}
			if len(routes) == 0 {
				delete(routeInfo.PodExternalRoutes, podIP)
				continue
			}
			for gw, gr := range routes {
				if !policyGWIPs.Has(gw) {
					if err := oc.deletePodGWRoute(routeInfo, podIP, gw, gr); err != nil {
						// if we encounter error while deleting routes for one pod; we return and don't try subsequent pods
						return fmt.Errorf("delete pod GW route failed: %w", err)
					}
					delete(routes, gw)
				}
			}
		}
		return nil
	})
}

// addEgressGwRoutesForPod handles adding all routes to gateways for a pod on a specific GR
func (oc *DefaultNetworkController) addGWRoutesForPod(gateways []*gatewayInfo, podIfAddrs []*net.IPNet, podNsName ktypes.NamespacedName, node string) error {
	pod, err := oc.watchFactory.PodCoreInformer().Lister().Pods(podNsName.Namespace).Get(podNsName.Name)
	if err != nil {
		return err
	}

	local, err := oc.isPodInLocalZone(pod)
	if err != nil {
		return err
	}
	if !local {
		klog.V(4).Infof("Not adding exgw routes for pod %s not in the local zone %s", podNsName, oc.zone)
		return nil
	}

	gr := oc.GetNetworkScopedGWRouterName(node)

	routesAdded := 0
	portPrefix, err := oc.extSwitchPrefix(node)
	if err != nil {
		klog.Infof("Failed to find ext switch prefix for %s %v", node, err)
		return err
	}

	port := portPrefix + types.GWRouterToExtSwitchPrefix + gr

	return oc.externalGatewayRouteInfo.CreateOrLoad(podNsName, func(routeInfo *apbroutecontroller.RouteInfo) error {
		policyGWIPs, err := oc.apbExternalRouteController.GetDynamicGatewayIPsForTargetNamespace(podNsName.Namespace)
		if err != nil {
			return err
		}
		policyStaticGWIPs, err := oc.apbExternalRouteController.GetStaticGatewayIPsForTargetNamespace(podNsName.Namespace)
		if err != nil {
			return err
		}
		policyGWIPs = policyGWIPs.Union(policyStaticGWIPs)

		for _, podIPNet := range podIfAddrs {
			for _, gateway := range gateways {
				// TODO (trozet): use the go bindings here and batch commands
				// validate the ip and gateway belong to the same address family
				gws, err := util.MatchAllIPStringFamily(utilnet.IsIPv6(podIPNet.IP), gateway.gws.UnsortedList())
				if err == nil {
					podIP := podIPNet.IP.String()
					for _, gw := range gws {
						// if route was already programmed, skip it
						foundGR, ok := routeInfo.PodExternalRoutes[podIP][gw]
						if (ok && foundGR == gr) || policyGWIPs.Has(gw) {
							routesAdded++
							continue
						}
						mask := util.GetIPFullMaskString(podIP)

						if err := oc.createBFDStaticRoute(gateway.bfdEnabled, gw, podIP, gr, port, mask); err != nil {
							return err
						}
						if routeInfo.PodExternalRoutes[podIP] == nil {
							routeInfo.PodExternalRoutes[podIP] = make(map[string]string)
						}
						routeInfo.PodExternalRoutes[podIP][gw] = gr
						routesAdded++
						if len(routeInfo.PodExternalRoutes[podIP]) == 1 {
							if err := oc.addHybridRoutePolicyForPod(podIPNet.IP, node); err != nil {
								return err
							}
						}
					}
				} else {
					klog.Warningf("Address families for the pod address %s and gateway %s did not match", podIPNet.IP.String(), gateway.gws)
				}
			}
		}
		// if no routes are added return an error
		if routesAdded < 1 {
			return fmt.Errorf("gateway specified for namespace %s with gateway addresses %v but no valid routes exist for pod: %s",
				podNsName.Namespace, podIfAddrs, podNsName.Name)
		}
		return nil
	})
}

// deletePodSNAT removes per pod SNAT rules towards the nodeIP that are applied to the GR where the pod resides
// used when disableSNATMultipleGWs=true
func (oc *DefaultNetworkController) deletePodSNAT(nodeName string, extIPs, podIPNets []*net.IPNet) error {

	node, err := oc.watchFactory.NodeCoreInformer().Lister().Get(nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If node does not exist, there is nothing to delete
			return nil
		}
		return err
	}
	if !oc.isLocalZoneNode(node) {
		klog.V(4).Infof("Node %s is not in the local zone %s", nodeName, oc.zone)
		return nil
	}
	// Default network does not set any matches in Pod SNAT
	ops, err := deletePodSNATOps(oc.nbClient, nil, oc.GetNetworkScopedGWRouterName(nodeName), extIPs, podIPNets)
	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to delete SNAT rule for pod on gateway router %s: %w", oc.GetNetworkScopedGWRouterName(nodeName), err)
	}
	return nil
}

// buildPodSNAT builds per pod SNAT rules towards the nodeIP that are applied to the GR where the pod resides.
// exemptedExtIPs should be an AddressSet UUID.
// When specified, traffic to IPs in that AddressSet will not be SNATed.
func buildPodSNAT(extIPs, podIPNets []*net.IPNet, match string, exemptedExtIPs string) ([]*nbdb.NAT, error) {
	nats := make([]*nbdb.NAT, 0, len(extIPs)*len(podIPNets))
	for _, podIPNet := range podIPNets {
		fullMaskPodNet := &net.IPNet{
			IP:   podIPNet.IP,
			Mask: util.GetIPFullMask(podIPNet.IP),
		}
		if len(extIPs) == 0 {
			nats = append(nats, libovsdbops.BuildSNATWithExemptedExtIPs(nil, fullMaskPodNet, "", nil, match, exemptedExtIPs))
		} else {
			for _, gwIPNet := range extIPs {
				if utilnet.IsIPv6CIDR(gwIPNet) != utilnet.IsIPv6CIDR(podIPNet) {
					continue
				}
				nats = append(nats, libovsdbops.BuildSNATWithExemptedExtIPs(&gwIPNet.IP, fullMaskPodNet, "", nil, match, exemptedExtIPs))
			}
		}
	}
	return nats, nil
}

// getExternalIPsGR returns all the externalIPs for a node(GR) from its l3 gateway annotation
func getExternalIPsGR(watchFactory *factory.WatchFactory, nodeName string) ([]*net.IPNet, error) {
	var err error
	node, err := watchFactory.GetNode(nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %v", nodeName, err)
	}
	l3GWConfig, err := util.ParseNodeL3GatewayAnnotation(node)
	if err != nil {
		return nil, fmt.Errorf("unable to parse node L3 gw annotation: %v", err)
	}
	return l3GWConfig.IPAddresses, nil
}

// deletePodSNATOps creates ovsdb operation that removes per pod SNAT rules towards the nodeIP that are applied to the GR where the pod resides
// used when disableSNATMultipleGWs=true
func deletePodSNATOps(nbClient libovsdbclient.Client, ops []ovsdb.Operation, gwRouterName string, extIPs, podIPNets []*net.IPNet) ([]ovsdb.Operation, error) {
	nats, err := buildPodSNAT(extIPs, podIPNets, "", "") // for delete, match and exemptedExtIPs are not needed - we try to cleanup all the SNATs that match the isEquivalentNAT predicate
	if err != nil {
		return nil, err
	}
	logicalRouter := nbdb.LogicalRouter{
		Name: gwRouterName,
	}
	ops, err = libovsdbops.DeleteNATsOps(nbClient, ops, &logicalRouter, nats...)
	if err != nil && !errors.Is(err, libovsdbclient.ErrNotFound) {
		return nil, fmt.Errorf("failed create operation for deleting SNAT rule for pod on gateway router %s: %v", logicalRouter.Name, err)
	}
	return ops, nil
}

// addOrUpdatePodSNAT adds or updates per pod SNAT rules towards the nodeIP that are applied to the GR where the pod resides
// used when disableSNATMultipleGWs=true
func addOrUpdatePodSNAT(nbClient libovsdbclient.Client, gwRouterName string, extIPs, podIfAddrs []*net.IPNet) error {
	ops, err := addOrUpdatePodSNATOps(nbClient, gwRouterName, extIPs, podIfAddrs, "", "", nil)
	if err != nil {
		return err
	}
	if _, err = libovsdbops.TransactAndCheck(nbClient, ops); err != nil {
		return fmt.Errorf("failed to update SNAT for pods of router %s: %v", gwRouterName, err)
	}
	return nil
}

// addOrUpdatePodSNATOps returns the operation that adds or updates per pod SNAT rules towards the nodeIP that are
// applied to the GR where the pod resides.
// exemptedExtIPs should be an AddressSet UUID.
// When specified, traffic to IPs in that AddressSet will not be SNATed.
// used when disableSNATMultipleGWs=true
func addOrUpdatePodSNATOps(nbClient libovsdbclient.Client, gwRouterName string, extIPs, podIfAddrs []*net.IPNet, snatMatch string, exemptedExtIPs string, ops []ovsdb.Operation) ([]ovsdb.Operation, error) {
	gwRouter := &nbdb.LogicalRouter{Name: gwRouterName}
	nats, err := buildPodSNAT(extIPs, podIfAddrs, snatMatch, exemptedExtIPs)
	if err != nil {
		return nil, err
	}
	if ops, err = libovsdbops.CreateOrUpdateNATsOps(nbClient, ops, gwRouter, nats...); err != nil {
		return nil, fmt.Errorf("failed to create ops to update SNAT for pods of router: %s, error: %v", gwRouterName, err)
	}
	return ops, nil
}

// addHybridRoutePolicyForPod handles adding a higher priority allow policy to allow traffic to be routed normally
// by ecmp routes.
// WARNING: updates same db entries as apbroutecontroller. Make sure to call only when route is not managed by
// apbroute controller.
func (oc *DefaultNetworkController) addHybridRoutePolicyForPod(podIP net.IP, node string) error {
	if config.Gateway.Mode == config.GatewayModeLocal {
		// Add podIP to the node's address_set.
		asIndex := apbroutecontroller.GetHybridRouteAddrSetDbIDs(node, oc.controllerName)
		as, err := oc.addressSetFactory.EnsureAddressSet(asIndex)
		if err != nil {
			return fmt.Errorf("cannot ensure that addressSet for node %s exists %v", node, err)
		}
		err = as.AddAddresses([]string{podIP.String()})
		if err != nil {
			return fmt.Errorf("unable to add PodIP %s: to the address set %s, err: %v", podIP.String(), node, err)
		}

		// add allow policy to bypass lr-policy in GR
		ipv4HashedAS, ipv6HashedAS := as.GetASHashNames()
		var l3Prefix string
		var matchSrcAS string
		isIPv6 := utilnet.IsIPv6(podIP)
		if isIPv6 {
			l3Prefix = "ip6"
			matchSrcAS = ipv6HashedAS
		} else {
			l3Prefix = "ip4"
			matchSrcAS = ipv4HashedAS
		}

		// get the GR to join switch ip address
		grJoinIfAddrs, err := libovsdbutil.GetLRPAddrs(oc.nbClient, types.GWRouterToJoinSwitchPrefix+oc.GetNetworkScopedGWRouterName(node))
		if err != nil {
			return fmt.Errorf("unable to find IP address for node: %s, %s port, err: %v", node, types.GWRouterToJoinSwitchPrefix, err)
		}
		grJoinIfAddr, err := util.MatchFirstIPNetFamily(utilnet.IsIPv6(podIP), grJoinIfAddrs)
		if err != nil {
			return fmt.Errorf("failed to match gateway router join interface IPs: %v, err: %v", grJoinIfAddr, err)
		}

		var matchDst string
		var clusterL3Prefix string
		for _, clusterSubnet := range config.Default.ClusterSubnets {
			if utilnet.IsIPv6CIDR(clusterSubnet.CIDR) {
				clusterL3Prefix = "ip6"
			} else {
				clusterL3Prefix = "ip4"
			}
			if l3Prefix != clusterL3Prefix {
				continue
			}
			matchDst += fmt.Sprintf(" && %s.dst != %s", clusterL3Prefix, clusterSubnet.CIDR)
		}

		// traffic destined outside of cluster subnet go to GR
		matchStr := fmt.Sprintf(`inport == "%s%s" && %s.src == $%s`, types.RouterToSwitchPrefix, node, l3Prefix, matchSrcAS)
		matchStr += matchDst

		logicalRouterPolicy := nbdb.LogicalRouterPolicy{
			Priority: types.HybridOverlayReroutePriority,
			Action:   nbdb.LogicalRouterPolicyActionReroute,
			Nexthops: []string{grJoinIfAddr.IP.String()},
			Match:    matchStr,
		}
		p := func(item *nbdb.LogicalRouterPolicy) bool {
			return item.Priority == logicalRouterPolicy.Priority && strings.Contains(item.Match, matchSrcAS)
		}
		err = libovsdbops.CreateOrUpdateLogicalRouterPolicyWithPredicate(oc.nbClient, oc.GetNetworkScopedClusterRouterName(),
			&logicalRouterPolicy, p, &logicalRouterPolicy.Nexthops, &logicalRouterPolicy.Match, &logicalRouterPolicy.Action)
		if err != nil {
			return fmt.Errorf("failed to add policy route %+v to %s: %v", logicalRouterPolicy, oc.GetNetworkScopedClusterRouterName(), err)
		}
	}
	return nil
}

// delHybridRoutePolicyForPod handles deleting a logical route policy that
// forces pod egress traffic to be rerouted to a gateway router for local gateway mode.
// WARNING: updates same db entries as apbroutecontroller. Make sure to call only when route is not managed by
// apbroute controller.
func (oc *DefaultNetworkController) delHybridRoutePolicyForPod(podIP net.IP, node string) error {
	if config.Gateway.Mode == config.GatewayModeLocal {
		// Delete podIP from the node's address_set.
		asIndex := apbroutecontroller.GetHybridRouteAddrSetDbIDs(node, oc.controllerName)
		as, err := oc.addressSetFactory.EnsureAddressSet(asIndex)
		if err != nil {
			return fmt.Errorf("cannot Ensure that addressSet for node %s exists %v", node, err)
		}
		err = as.DeleteAddresses([]string{podIP.String()})
		if err != nil {
			return fmt.Errorf("unable to remove PodIP %s: to the address set %s, err: %v", podIP, node, err)
		}

		// delete hybrid policy to bypass lr-policy in GR, only if there are zero pods on this node.
		ipv4HashedAS, ipv6HashedAS := as.GetASHashNames()
		ipv4PodIPs, ipv6PodIPs := as.GetAddresses()
		deletePolicy := false
		var l3Prefix string
		var matchSrcAS string
		if utilnet.IsIPv6(podIP) {
			l3Prefix = "ip6"
			if len(ipv6PodIPs) == 0 {
				deletePolicy = true
			}
			matchSrcAS = ipv6HashedAS
		} else {
			l3Prefix = "ip4"
			if len(ipv4PodIPs) == 0 {
				deletePolicy = true
			}
			matchSrcAS = ipv4HashedAS
		}
		if deletePolicy {
			var matchDst string
			var clusterL3Prefix string
			for _, clusterSubnet := range config.Default.ClusterSubnets {
				if utilnet.IsIPv6CIDR(clusterSubnet.CIDR) {
					clusterL3Prefix = "ip6"
				} else {
					clusterL3Prefix = "ip4"
				}
				if l3Prefix != clusterL3Prefix {
					continue
				}
				matchDst += fmt.Sprintf(" && %s.dst != %s", l3Prefix, clusterSubnet.CIDR)
			}
			matchStr := fmt.Sprintf(`inport == "%s%s" && %s.src == $%s`, types.RouterToSwitchPrefix, node, l3Prefix, matchSrcAS)
			matchStr += matchDst

			p := func(item *nbdb.LogicalRouterPolicy) bool {
				return item.Priority == types.HybridOverlayReroutePriority && item.Match == matchStr
			}
			err := libovsdbops.DeleteLogicalRouterPoliciesWithPredicate(oc.nbClient, oc.GetNetworkScopedClusterRouterName(), p)
			if err != nil {
				return fmt.Errorf("error deleting policy %s on router %s: %v", matchStr, oc.GetNetworkScopedClusterRouterName(), err)
			}
		}
		if len(ipv4PodIPs) == 0 && len(ipv6PodIPs) == 0 {
			// delete address set.
			err := as.Destroy()
			if err != nil {
				return fmt.Errorf("failed to remove address set: %s, on: %s, err: %v",
					as.GetName(), node, err)
			}
		}
	}
	return nil
}

// delAllHybridRoutePolicies deletes all the 501 hybrid-route-policies that
// force pod egress traffic to be rerouted to a gateway router for local gateway mode.
// Called when migrating to SGW from LGW.
func (oc *DefaultNetworkController) delAllHybridRoutePolicies() error {
	// nuke all the policies
	policyPred := func(item *nbdb.LogicalRouterPolicy) bool {
		return item.Priority == types.HybridOverlayReroutePriority
	}
	err := libovsdbops.DeleteLogicalRouterPoliciesWithPredicate(oc.nbClient, oc.GetNetworkScopedClusterRouterName(), policyPred)
	if err != nil {
		return fmt.Errorf("error deleting hybrid route policies on %s: %v", oc.GetNetworkScopedClusterRouterName(), err)
	}

	// nuke all the address-sets.
	// if we fail to remove LRP's above, we don't attempt to remove ASes due to dependency constraints.
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetHybridNodeRoute, oc.controllerName, nil)
	asPred := libovsdbops.GetPredicate[*nbdb.AddressSet](predicateIDs, nil)
	err = libovsdbops.DeleteAddressSetsWithPredicate(oc.nbClient, asPred)
	if err != nil {
		return fmt.Errorf("failed to remove hybrid route address sets: %v", err)
	}

	return nil
}

// delAllLegacyHybridRoutePolicies deletes all the 501 hybrid-route-policies that
// force pod egress traffic to be rerouted to a gateway router for local gateway mode.
// New hybrid route matches on address set, while legacy matches just on pod IP
func (oc *DefaultNetworkController) delAllLegacyHybridRoutePolicies() error {
	// nuke all the policies
	p := func(item *nbdb.LogicalRouterPolicy) bool {
		if item.Priority != types.HybridOverlayReroutePriority {
			return false
		}
		if isNewVer, err := regexp.MatchString(`src\s*==\s*\$`, item.Match); err == nil && isNewVer {
			return false
		}
		return true
	}
	err := libovsdbops.DeleteLogicalRouterPoliciesWithPredicate(oc.nbClient, oc.GetNetworkScopedClusterRouterName(), p)
	if err != nil {
		return fmt.Errorf("error deleting legacy hybrid route policies on %s: %v", oc.GetNetworkScopedClusterRouterName(), err)
	}

	return nil
}

// cleanUpBFDEntry checks if the BFD table entry related to the associated
// gw router / port / gateway ip is referenced by other routing rules, and if
// not removes the entry to avoid having dangling BFD entries.
func (oc *DefaultNetworkController) cleanUpBFDEntry(gatewayIP, gatewayRouter, prefix string) error {
	portName := prefix + types.GWRouterToExtSwitchPrefix + gatewayRouter
	p := func(item *nbdb.LogicalRouterStaticRoute) bool {
		return item.OutputPort != nil && *item.OutputPort == portName && item.Nexthop == gatewayIP && item.BFD != nil && *item.BFD != ""
	}
	logicalRouterStaticRoutes, err := libovsdbops.FindLogicalRouterStaticRoutesWithPredicate(oc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cleanUpBFDEntry failed to list routes for %s: %w", portName, err)
	}

	if len(logicalRouterStaticRoutes) > 0 {
		return nil
	}

	bfd := nbdb.BFD{
		LogicalPort: portName,
		DstIP:       gatewayIP,
	}

	err = libovsdbops.DeleteBFDs(oc.nbClient, &bfd)
	if err != nil {
		return fmt.Errorf("error deleting BFD %+v: %v", bfd, err)
	}

	return nil
}

// extSwitchPrefix returns the prefix of the external switch to use for
// external gateway routes. In case no second bridge is configured, we
// use the default one and the prefix is empty.
func (oc *DefaultNetworkController) extSwitchPrefix(nodeName string) (string, error) {
	node, err := oc.watchFactory.GetNode(nodeName)
	if err != nil {
		return "", fmt.Errorf("extSwitchPrefix failed to find node %s: %w", nodeName, err)
	}
	l3GatewayConfig, err := util.ParseNodeL3GatewayAnnotation(node)
	if err != nil {
		return "", fmt.Errorf("extSwitchPrefix failed to parse l3 gateway annotation for node %s: %w", nodeName, err)
	}

	if l3GatewayConfig.EgressGWInterfaceID != "" {
		return types.EgressGWSwitchPrefix, nil
	}
	return "", nil
}

func getExGwPodIPs(gatewayPod *corev1.Pod) (sets.Set[string], error) {
	foundGws := sets.New[string]()
	if gatewayPod.Annotations[util.RoutingNetworkAnnotation] != "" {
		var multusNetworks []nettypes.NetworkStatus
		err := json.Unmarshal([]byte(gatewayPod.ObjectMeta.Annotations[nettypes.NetworkStatusAnnot]), &multusNetworks)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall annotation k8s.v1.cni.cncf.io/network-status on pod %s: %v",
				gatewayPod.Name, err)
		}
		for _, multusNetwork := range multusNetworks {
			if multusNetwork.Name == gatewayPod.Annotations[util.RoutingNetworkAnnotation] {
				for _, gwIP := range multusNetwork.IPs {
					ip := net.ParseIP(gwIP)
					if ip != nil {
						foundGws.Insert(ip.String())
					}
				}
			}
		}
	} else if gatewayPod.Spec.HostNetwork {
		for _, podIP := range gatewayPod.Status.PodIPs {
			ip := utilnet.ParseIPSloppy(podIP.IP)
			if ip != nil {
				foundGws.Insert(ip.String())
			}
		}
	} else {
		return nil, fmt.Errorf("ignoring pod %s as an external gateway candidate. Invalid combination "+
			"of host network: %t and routing-network annotation: %s", gatewayPod.Name, gatewayPod.Spec.HostNetwork,
			gatewayPod.Annotations[util.RoutingNetworkAnnotation])
	}
	return foundGws, nil
}

func makePodGWKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
}
