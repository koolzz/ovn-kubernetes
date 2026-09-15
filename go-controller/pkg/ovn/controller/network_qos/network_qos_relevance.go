// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package networkqos

import (
	"k8s.io/klog/v2"

	networkqosapi "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/networkqos/v1alpha1"
)

// refreshNetworkQoSRelevance runs on selector inputs, never on Pod events.
// Publish relevance before queuing policies: their resync covers Pod events
// skipped before this refresh, and later Pod events must not be skipped while
// that resync is in progress. Reconciled state cannot provide this guarantee.
func (c *Controller) refreshNetworkQoSRelevance() []*networkqosapi.NetworkQoS {
	c.relevanceMutex.Lock()
	defer c.relevanceMutex.Unlock()

	policies, err := c.getAllNetworkQoSes()
	if err != nil {
		c.hasRelevantPolicy.Store(true)
		klog.Errorf("%s: failed to refresh NetworkQoS relevance: %v", c.controllerName, err)
		return nil
	}
	relevant := false
	for _, policy := range policies {
		matches, err := c.networkManagedByMe(policy.Spec.NetworkSelectors)
		if err != nil || matches {
			// Lookup/selector errors must leave Pod events enabled so normal
			// reconciliation can report the error and retry.
			relevant = true
			break
		}
	}
	if c.hasRelevantPolicy.Swap(relevant) != relevant {
		// A Namespace or NAD change can activate the first policy or make
		// the last one irrelevant. Resync/cleanup must not depend on another
		// Pod event arriving after that change.
		for _, policy := range policies {
			c.nqosQueue.Add(joinMetaNamespaceAndName(policy.Namespace, policy.Name))
		}
	}
	return policies
}

func (c *Controller) onNQOSNADChange(_ interface{}) {
	// NAD labels, ownership and network identity can change selection even
	// while the aggregate relevance flag remains true.
	for _, policy := range c.refreshNetworkQoSRelevance() {
		c.nqosQueue.Add(joinMetaNamespaceAndName(policy.Namespace, policy.Name))
	}
}
