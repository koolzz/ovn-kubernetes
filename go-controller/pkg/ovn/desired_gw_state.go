// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"sort"
)

// desiredGWState is the per-namespace desired gateway state used by the
// namespace-driven apply primitive. It tracks each gateway IP together
// with its BFD setting; equality is per (IP, BFD) tuple, not per IP set.
// This matters because a route's BFD flag is part of the OVN static
// route's identity at the libovsdb level — a namespace whose gw IP set
// is unchanged but whose BFD flag flips still needs route reconvergence
// (delete + re-add per affected IP).
//
// When the same IP arrives from multiple sources (annotation +
// gateway-pod) with conflicting BFD flags, the merge is
// OR-on-collision: any source asking for BFD on an IP makes the merged
// BFD true. This makes the result deterministic and order-independent —
// previously the source that called the route primitive first won by
// virtue of the PodExternalRoutes short-circuit. See
// namespace-migration-plan.md §1b for the rationale.
type desiredGWState struct {
	// gws maps gateway IP to BFD-enabled.
	gws map[string]bool
}

// newDesiredGWState returns an empty desiredGWState ready for addGW
// calls.
func newDesiredGWState() *desiredGWState {
	return &desiredGWState{gws: map[string]bool{}}
}

// addGW records that the given gateway IP should be programmed for the
// namespace, with the given BFD setting. If the IP is already present
// the BFD flag is OR'd with the existing value.
func (d *desiredGWState) addGW(ip string, bfd bool) {
	if d.gws == nil {
		d.gws = map[string]bool{}
	}
	d.gws[ip] = d.gws[ip] || bfd
}

// addAnnotationGWs merges all IPs from a namespace-annotation-derived
// gatewayInfo (a single BFD flag applies to every IP). A nil or empty
// set is a no-op.
func (d *desiredGWState) addAnnotationGWs(info gatewayInfo) {
	if info.gws == nil {
		return
	}
	for ip := range info.gws {
		d.addGW(ip, info.bfdEnabled)
	}
}

// addPodGWs merges a pod-derived gateway set in the (IP -> BFD) shape
// returned by gatewayPodIndex.GatewaysForNamespace. A nil or empty map
// is a no-op.
func (d *desiredGWState) addPodGWs(podGWs map[string]bool) {
	for ip, bfd := range podGWs {
		d.addGW(ip, bfd)
	}
}

// equal reports whether two states agree on every (IP, BFD) tuple.
// Order-independent. nil and empty map are equal.
func (d *desiredGWState) equal(other *desiredGWState) bool {
	a, b := d.size(), other.size()
	if a != b {
		return false
	}
	if a == 0 {
		return true
	}
	for ip, bfd := range d.gws {
		otherBFD, ok := other.gws[ip]
		if !ok || otherBFD != bfd {
			return false
		}
	}
	return true
}

// size returns the number of IPs in the desired state. Treats nil
// receiver and nil map as size 0.
func (d *desiredGWState) size() int {
	if d == nil {
		return 0
	}
	return len(d.gws)
}

// ipSet returns a fresh sorted slice of the gateway IPs in the desired
// state. Useful for callers that need just the IP universe (e.g.
// conntrack flush) and don't care about per-IP BFD.
func (d *desiredGWState) ipSet() []string {
	if d == nil || len(d.gws) == 0 {
		return nil
	}
	out := make([]string, 0, len(d.gws))
	for ip := range d.gws {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out
}

// computeDesiredGWState builds the namespace's desired gateway state by
// merging the annotation-derived GWs and the pod-derived GWs from the
// gateway-pod index. BFD flags are OR'd on collision per addGW. Either
// input may be empty; an empty result is valid and represents "no
// gateways desired".
func computeDesiredGWState(annotation gatewayInfo, podGWs map[string]bool) *desiredGWState {
	d := newDesiredGWState()
	d.addAnnotationGWs(annotation)
	d.addPodGWs(podGWs)
	return d
}
