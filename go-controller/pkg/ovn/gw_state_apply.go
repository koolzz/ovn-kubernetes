// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// gwRouteProgrammer is the minimal route-programming surface the
// namespace-driven apply primitive needs. The default network's
// add/deleteGWRoutesForNamespace methods satisfy it. Extracted so the
// apply primitive can be unit-tested without a full fakeOVN setup —
// the real semantics of route creation/deletion belong to the existing
// methods, which are exercised by the gateway-pod integration tests
// already.
type gwRouteProgrammer interface {
	// addRoutesForNamespace programs OVN static routes for every IP
	// in info.gws into ns, with the supplied BFD flag uniformly. The
	// underlying primitive is idempotent: an IP that's already
	// programmed via the same gateway router is skipped.
	addRoutesForNamespace(ns string, info gatewayInfo) error
	// deleteRoutesForNamespace removes OVN static routes whose
	// gateway IP is in matchGWs. Pass an empty/nil set to delete all
	// gateway routes for the namespace.
	deleteRoutesForNamespace(ns string, matchGWs sets.Set[string]) error
}

// gwIPBFD pairs a gateway IP with its BFD flag. Used for the add/replace
// legs of a gateway-state delta where per-IP BFD matters.
type gwIPBFD struct {
	ip  string
	bfd bool
}

// gwStateDelta describes the changes required to bring the per-namespace
// applied gateway state to the desired state. Three legs:
//
// - add: IPs present in desired but not in applied. Pure additions.
// - remove: IPs present in applied but not in desired. Pure deletions.
// - replace: IPs present in both, but BFD flag differs. Delete the old
//   route and add the new one — must be one libovsdb transaction so
//   traffic doesn't fall into a window with no route for the IP.
//
// An empty delta (all three slices empty) means applied == desired and
// no programming work is required.
type gwStateDelta struct {
	add     []gwIPBFD
	remove  []string
	replace []gwIPBFD
}

// empty reports whether the delta has any work to do.
func (d gwStateDelta) empty() bool {
	return len(d.add) == 0 && len(d.remove) == 0 && len(d.replace) == 0
}

// computeGWStateDelta returns the diff that brings applied to desired.
// A nil applied is equivalent to "nothing programmed yet" — every
// desired IP becomes an add. A nil desired means "remove everything" —
// every applied IP becomes a remove. Both nil yields an empty delta.
//
// Output slices are sorted by IP for deterministic ordering (helps both
// tests and operator-visible logs).
func computeGWStateDelta(applied, desired *desiredGWState) gwStateDelta {
	var delta gwStateDelta
	for ip, bfd := range desired.entries() {
		appliedBFD, ok := applied.entries()[ip]
		switch {
		case !ok:
			delta.add = append(delta.add, gwIPBFD{ip: ip, bfd: bfd})
		case appliedBFD != bfd:
			delta.replace = append(delta.replace, gwIPBFD{ip: ip, bfd: bfd})
		}
	}
	for ip := range applied.entries() {
		if _, ok := desired.entries()[ip]; !ok {
			delta.remove = append(delta.remove, ip)
		}
	}
	sortGWIPBFD(delta.add)
	sortGWIPBFD(delta.replace)
	sort.Strings(delta.remove)
	return delta
}

// entries returns the underlying map. Nil-receiver-safe.
func (d *desiredGWState) entries() map[string]bool {
	if d == nil {
		return nil
	}
	return d.gws
}

func sortGWIPBFD(s []gwIPBFD) {
	sort.Slice(s, func(i, j int) bool { return s[i].ip < s[j].ip })
}

// nsAppliedGWState tracks the last successfully-applied gateway state
// per namespace, scoped to a single controller (the default network is
// the only consumer today; UDNs don't have gateway pods, so they have
// no applied-state to track). Reads/writes are safe for concurrent use.
//
// Bootstrap rule: callers must seed this from NBDB before the first
// per-namespace reconcile fires after a controller restart, otherwise a
// namespace with empty desired state and stale OVN routes would compare
// applied.equal(desired) trivially and skip the delete that should fire.
type nsAppliedGWState struct {
	mu sync.RWMutex
	// byNamespace is keyed by namespace name. A nil value means "no
	// known applied state for this namespace" — distinct from "applied
	// state is empty" (an explicit empty desiredGWState). The
	// distinction matters during bootstrap: if NBDB seeding has not
	// yet recorded a namespace, the snapshot must not be treated as
	// "empty" (which would mask stale-route deletes).
	byNamespace map[string]*desiredGWState
}

func newNSAppliedGWState() *nsAppliedGWState {
	return &nsAppliedGWState{byNamespace: map[string]*desiredGWState{}}
}

// Get returns the last-applied state for the namespace, or nil if no
// state has been recorded. The returned pointer is the live cache
// entry; callers must treat it as read-only.
func (s *nsAppliedGWState) Get(ns string) *desiredGWState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byNamespace[ns]
}

// Set records the applied state for the namespace. The caller hands
// ownership of state to the cache; callers should not mutate it after
// Set. A nil state clears the entry (equivalent to Delete).
func (s *nsAppliedGWState) Set(ns string, state *desiredGWState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state == nil {
		delete(s.byNamespace, ns)
		return
	}
	s.byNamespace[ns] = state
}

// Delete removes the applied state for the namespace.
func (s *nsAppliedGWState) Delete(ns string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byNamespace, ns)
}

// Has reports whether the cache has recorded any state for the
// namespace. Returns true even when the recorded state is empty.
func (s *nsAppliedGWState) Has(ns string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.byNamespace[ns]
	return ok
}

// Namespaces returns a sorted list of namespaces with recorded applied
// state. Useful for bootstrap reconciliation that needs to walk every
// known namespace (e.g., to detect annotations that have been cleared
// while the controller was down).
func (s *nsAppliedGWState) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.byNamespace) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.byNamespace))
	for ns := range s.byNamespace {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

// applyGWStateDelta drives the IP-level reconvergence required to bring
// applied state to desired state for a single namespace. It schedules:
//
//   1. one delete pass covering both pure-removes and BFD-replace IPs,
//      so the BFD-replace re-add picks up the new flag rather than
//      short-circuiting on the existing-route check inside
//      addGWRoutesForPod;
//   2. one add pass per BFD flag value, since the underlying primitive
//      takes a single BFD flag for all IPs in its gatewayInfo.
//
// Order is delete-first to minimize the no-route window for replace
// IPs. A single libovsdb transaction across delete + add is the ideal
// (covered by Phase 1b.6 follow-up); today's primitives execute their
// own transactions, so the BFD-flip case has a brief window where the
// IP has no route. The window is bounded by one transaction round-trip
// and only fires when an operator changes the BFD flag while the IP
// itself is stable — a rare path.
func applyGWStateDelta(ns string, delta gwStateDelta, p gwRouteProgrammer) error {
	if delta.empty() {
		return nil
	}
	deleteIPs := sets.New[string](delta.remove...)
	for _, e := range delta.replace {
		deleteIPs.Insert(e.ip)
	}
	if deleteIPs.Len() > 0 {
		if err := p.deleteRoutesForNamespace(ns, deleteIPs); err != nil {
			return err
		}
	}
	addByBFD := groupAddsByBFD(delta)
	for _, bfd := range []bool{false, true} {
		ips, ok := addByBFD[bfd]
		if !ok || ips.Len() == 0 {
			continue
		}
		if err := p.addRoutesForNamespace(ns, gatewayInfo{
			gws:        ips,
			bfdEnabled: bfd,
		}); err != nil {
			return err
		}
	}
	return nil
}

// groupAddsByBFD merges delta.add and delta.replace into per-BFD-flag
// IP sets. Replace IPs land in this same map because their re-add is
// indistinguishable from an add for the purposes of the underlying
// primitive — the preceding delete pass cleared the old route.
func groupAddsByBFD(delta gwStateDelta) map[bool]sets.Set[string] {
	if len(delta.add) == 0 && len(delta.replace) == 0 {
		return nil
	}
	out := map[bool]sets.Set[string]{}
	for _, e := range delta.add {
		if out[e.bfd] == nil {
			out[e.bfd] = sets.New[string]()
		}
		out[e.bfd].Insert(e.ip)
	}
	for _, e := range delta.replace {
		if out[e.bfd] == nil {
			out[e.bfd] = sets.New[string]()
		}
		out[e.bfd].Insert(e.ip)
	}
	return out
}
