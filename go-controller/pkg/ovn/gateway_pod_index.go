// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1pod "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// gatewayPodPayload is the per-gateway-pod state that namespace reconcile
// reads. It is computed purely from the pod object — annotations, status,
// and pod-derived IPs — so two snapshots produced from the same pod
// observation always compare equal.
//
// "Active" gateway-pods (those whose payload contributes to the namespace
// desired-state) require all of: a non-empty target-namespace set, a
// non-empty resolved gateway IP set, ready=true, and not terminating.
// Pods that fail any of these checks have their entry kept in the index
// only as long as we need to remember a prior "active" payload to
// generate a deletion delta — once that's been processed, they get
// removed.
type gatewayPodPayload struct {
	// targetNS is the parsed comma-split routing-namespaces annotation.
	targetNS sets.Set[string]
	// gws is the resolved set of gateway IPs (from network-status or
	// pod IPs depending on the routing-network annotation).
	gws sets.Set[string]
	// bfdEnabled is the pod-level BfdAnnotation flag.
	bfdEnabled bool
	// ready is true when the pod is ready and not terminating. Pods
	// with ready=false should not contribute to the namespace
	// desired-state.
	ready bool
	// network is the routing-network annotation value (empty for
	// host-network gateway pods). Tracked so a network-change is
	// detected as a payload change.
	network string
	// active is true when this payload should contribute to the
	// namespace desired-state. False payloads are kept transiently so
	// the next reconcile knows to enqueue the prior target set for
	// deletion.
	active bool
}

// equal returns true when two payloads are observably the same. Equality
// is order-independent for the set fields.
func (p gatewayPodPayload) equal(other gatewayPodPayload) bool {
	if p.bfdEnabled != other.bfdEnabled {
		return false
	}
	if p.ready != other.ready {
		return false
	}
	if p.network != other.network {
		return false
	}
	if p.active != other.active {
		return false
	}
	if !p.targetNS.Equal(other.targetNS) {
		return false
	}
	if !p.gws.Equal(other.gws) {
		return false
	}
	return true
}

// gatewayPodIndex tracks "what gateway pods serve which namespaces" in a
// way that namespace reconcile can read O(1). It owns:
//
//   - a forward map (pod-key → last-observed payload),
//   - a reverse map (target namespace → set of gateway-pod keys).
//
// Single-writer (the gateway-pod reconcile path); many-readers (namespace
// reconcile, conntrack flusher, APB merge). Synchronization via RWMutex;
// readers always work on copies (sets and maps), never references into
// internal storage.
//
// Bootstrap: HasSynced returns true only after BootstrapFromPodList is
// called at least once. Callers that depend on the index for correctness
// (notably any namespace reconcile that computes a desired-state delete
// from "applied vs current") MUST gate on HasSynced before using the
// index — otherwise a controller restart with stale routes can compute
// an empty desired-state and delete legitimate routes.
type gatewayPodIndex struct {
	mu sync.RWMutex
	// forward: pod key → last observed payload.
	payload map[string]gatewayPodPayload
	// reverse: target namespace → set of pod keys.
	byNamespace map[string]sets.Set[string]
	// hasSynced flips to true after the first BootstrapFromPodList.
	hasSynced bool
}

// newGatewayPodIndex constructs an empty index.
func newGatewayPodIndex() *gatewayPodIndex {
	return &gatewayPodIndex{
		payload:     map[string]gatewayPodPayload{},
		byNamespace: map[string]sets.Set[string]{},
	}
}

// HasSynced reports whether BootstrapFromPodList has been called. The
// gating contract is: namespace reconcile MUST observe HasSynced == true
// before reading the index for desired-state computation.
func (g *gatewayPodIndex) HasSynced() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.hasSynced
}

// computePayload derives a payload from the pod object. Returns the
// payload and a sentinel for whether the pod is *currently* a gateway-pod
// candidate at all (i.e., has the routing-namespaces annotation).
//
// "active" requires the candidate to also be ready, non-terminating, and
// to have at least one resolved GW IP.
func computeGatewayPodPayload(pod *corev1.Pod) (gatewayPodPayload, bool) {
	rawTargets := pod.Annotations[util.RoutingNamespaceAnnotation]
	if rawTargets == "" {
		return gatewayPodPayload{}, false
	}
	targetNS := sets.New[string]()
	for _, t := range splitCommaTrim(rawTargets) {
		if t != "" {
			targetNS.Insert(t)
		}
	}
	if targetNS.Len() == 0 {
		return gatewayPodPayload{targetNS: targetNS}, true
	}

	_, bfd := pod.Annotations[util.BfdAnnotation]
	network := pod.Annotations[util.RoutingNetworkAnnotation]
	ready := !util.PodTerminating(pod) && v1pod.IsPodReadyConditionTrue(pod.Status)

	gws := sets.New[string]()
	if ready {
		// Resolution failures here drop ready→false rather than
		// crashing; the pod stays "candidate" but inactive.
		resolved, err := getExGwPodIPs(pod)
		if err != nil || resolved.Len() == 0 {
			ready = false
		} else {
			gws = resolved
		}
	}

	active := ready && targetNS.Len() > 0 && gws.Len() > 0
	return gatewayPodPayload{
		targetNS:   targetNS,
		gws:        gws,
		bfdEnabled: bfd,
		ready:      ready,
		network:    network,
		active:     active,
	}, true
}

// splitCommaTrim splits s on "," and trims whitespace; empty entries are
// included as empty strings (caller filters).
func splitCommaTrim(s string) []string {
	if s == "" {
		return nil
	}
	out := make([]string, 0)
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			tok := s[start:i]
			// trim ASCII space
			for len(tok) > 0 && (tok[0] == ' ' || tok[0] == '\t') {
				tok = tok[1:]
			}
			for len(tok) > 0 && (tok[len(tok)-1] == ' ' || tok[len(tok)-1] == '\t') {
				tok = tok[:len(tok)-1]
			}
			out = append(out, tok)
			start = i + 1
		}
	}
	return out
}

// Update records the current observed payload for a pod and returns the
// union of the prior and new target-namespace sets (so the caller can
// fan-out namespace reconciles to every namespace that *was* or *is now*
// served by this pod). The returned set is a fresh copy.
//
// Update is the single writer entry point. It must complete before the
// caller enqueues namespace reconciles, so namespace reconcile reads the
// fresh payload and not a stale one.
//
// If the pod is not a gateway-pod candidate (no routing-namespaces
// annotation), the prior entry for podKey is removed and any prior
// targets are returned so the caller can drive the deletion.
func (g *gatewayPodIndex) Update(pod *corev1.Pod) sets.Set[string] {
	if pod == nil {
		return sets.New[string]()
	}
	podKey := makePodGWKey(pod)

	newPayload, isCandidate := computeGatewayPodPayload(pod)

	g.mu.Lock()
	defer g.mu.Unlock()

	old, hadOld := g.payload[podKey]

	if !isCandidate {
		// Pod is no longer a gateway-pod candidate. Remove forward
		// entry and reverse-map memberships; return the prior targets.
		g.removeLocked(podKey)
		if hadOld {
			return sets.New(old.targetNS.UnsortedList()...)
		}
		return sets.New[string]()
	}

	// Update forward map.
	g.payload[podKey] = newPayload

	// Reverse-map: remove from old targets that are no longer current,
	// add to new targets that weren't previously listed.
	if hadOld {
		for ns := range old.targetNS {
			if !newPayload.targetNS.Has(ns) {
				g.removeFromReverseLocked(ns, podKey)
			}
		}
	}
	for ns := range newPayload.targetNS {
		if !hadOld || !old.targetNS.Has(ns) {
			g.addToReverseLocked(ns, podKey)
		}
	}

	// Affected ns set: fan-out covers any ns that *was* or *is now* a
	// target, regardless of whether the payload changed in some other
	// dimension. Same-target payload changes (e.g., BFD toggle, ready
	// flip, IPs changed) still need fan-out.
	out := sets.New(newPayload.targetNS.UnsortedList()...)
	if hadOld {
		out.Insert(old.targetNS.UnsortedList()...)
	}
	return out
}

// Delete drops a pod entirely from the index (used on true pod deletion).
// Returns the set of target namespaces the pod was serving so the caller
// can enqueue namespace reconciles for deletion.
func (g *gatewayPodIndex) Delete(podKey string) sets.Set[string] {
	g.mu.Lock()
	defer g.mu.Unlock()
	old, ok := g.payload[podKey]
	if !ok {
		return sets.New[string]()
	}
	g.removeLocked(podKey)
	return sets.New(old.targetNS.UnsortedList()...)
}

// removeLocked drops podKey from forward + reverse maps. Caller holds mu.
func (g *gatewayPodIndex) removeLocked(podKey string) {
	old, ok := g.payload[podKey]
	if !ok {
		return
	}
	for ns := range old.targetNS {
		g.removeFromReverseLocked(ns, podKey)
	}
	delete(g.payload, podKey)
}

func (g *gatewayPodIndex) addToReverseLocked(ns, podKey string) {
	pods := g.byNamespace[ns]
	if pods == nil {
		pods = sets.New[string]()
		g.byNamespace[ns] = pods
	}
	pods.Insert(podKey)
}

func (g *gatewayPodIndex) removeFromReverseLocked(ns, podKey string) {
	pods := g.byNamespace[ns]
	if pods == nil {
		return
	}
	pods.Delete(podKey)
	if pods.Len() == 0 {
		delete(g.byNamespace, ns)
	}
}

// GatewaysForNamespace returns the merged desired GW set for the
// namespace, with per-IP BFD resolved via OR-on-collision (any source
// asking for BFD wins).
//
// Only payloads whose `active` flag is true contribute. Inactive
// payloads (e.g., not-ready gateway pods) are present in the index but
// don't count toward the desired-state.
//
// Returns a fresh map; callers may mutate it freely.
func (g *gatewayPodIndex) GatewaysForNamespace(ns string) map[string]bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := map[string]bool{}
	pods := g.byNamespace[ns]
	if pods == nil {
		return out
	}
	for podKey := range pods {
		p, ok := g.payload[podKey]
		if !ok || !p.active {
			continue
		}
		for ip := range p.gws {
			// OR-on-collision: any source asking for BFD on an IP
			// makes the merged BFD true.
			out[ip] = out[ip] || p.bfdEnabled
		}
	}
	return out
}

// PodsForNamespace returns a sorted copy of the gateway-pod keys
// currently serving the namespace. Useful for callers (e.g., conntrack
// flush, APB merge) that walk gateway pods directly.
func (g *gatewayPodIndex) PodsForNamespace(ns string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	pods := g.byNamespace[ns]
	if pods == nil {
		return nil
	}
	out := pods.UnsortedList()
	sort.Strings(out)
	return out
}

// PerPodGatewaysForNamespace returns a per-gateway-pod view of the
// gateways serving the namespace, keyed by gateway-pod key (matches
// the historic map[podKey]gatewayInfo shape). Only payloads with
// active==true are included.
//
// Returns a fresh map; values' gws sets are independent copies.
func (g *gatewayPodIndex) PerPodGatewaysForNamespace(ns string) map[string]gatewayInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := map[string]gatewayInfo{}
	pods := g.byNamespace[ns]
	if pods == nil {
		return out
	}
	for podKey := range pods {
		p, ok := g.payload[podKey]
		if !ok || !p.active {
			continue
		}
		out[podKey] = gatewayInfo{
			gws:        sets.New(p.gws.UnsortedList()...),
			bfdEnabled: p.bfdEnabled,
		}
	}
	return out
}

// PayloadFor returns a copy of the last-observed payload for a pod, or
// (zero, false) if the pod is not in the index. The returned sets are
// independent copies.
func (g *gatewayPodIndex) PayloadFor(podKey string) (gatewayPodPayload, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	p, ok := g.payload[podKey]
	if !ok {
		return gatewayPodPayload{}, false
	}
	return gatewayPodPayload{
		targetNS:   sets.New(p.targetNS.UnsortedList()...),
		gws:        sets.New(p.gws.UnsortedList()...),
		bfdEnabled: p.bfdEnabled,
		ready:      p.ready,
		network:    p.network,
		active:     p.active,
	}, true
}

// BootstrapFromPodList primes the index from the current pod-informer
// state and flips HasSynced to true. Must be called once at controller
// startup before namespace reconcile is allowed to consume the index.
//
// Calling BootstrapFromPodList more than once replaces the entire index
// with the new pods (caller-side use-case: drift recovery).
func (g *gatewayPodIndex) BootstrapFromPodList(pods []*corev1.Pod) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.payload = map[string]gatewayPodPayload{}
	g.byNamespace = map[string]sets.Set[string]{}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		payload, isCandidate := computeGatewayPodPayload(pod)
		if !isCandidate {
			continue
		}
		podKey := makePodGWKey(pod)
		g.payload[podKey] = payload
		for ns := range payload.targetNS {
			g.addToReverseLocked(ns, podKey)
		}
	}
	g.hasSynced = true
}

// String returns a stable human-readable summary; useful for debug logs
// and test failure output.
func (g *gatewayPodIndex) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	keys := make([]string, 0, len(g.payload))
	for k := range g.payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := "gatewayPodIndex{"
	for i, k := range keys {
		p := g.payload[k]
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s=>{ns=%v gws=%v bfd=%v ready=%v active=%v}",
			k, sortedList(p.targetNS), sortedList(p.gws), p.bfdEnabled, p.ready, p.active)
	}
	out += "}"
	return out
}

func sortedList(s sets.Set[string]) []string {
	out := s.UnsortedList()
	sort.Strings(out)
	return out
}
