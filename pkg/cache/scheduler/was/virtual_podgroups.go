//go:build !exclude_scheduler_library

/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package was

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
)

const (
	maxWASChildrenPerGroup = 8
	maxWASHierarchyDepth   = 4
)

type UnsupportableReason string

const (
	ReasonPreferredTopology      UnsupportableReason = "PreferredTopologyNotSupported"
	ReasonHierarchyDepthExceeded UnsupportableReason = "HierarchyDepthExceeded"
	ReasonChildLimitExceeded     UnsupportableReason = "ChildLimitExceeded"
)

type ErrUnsupportableWorkload struct {
	WorkloadName string
	Reason       UnsupportableReason
	Message      string
}

func (e *ErrUnsupportableWorkload) Error() string {
	return fmt.Sprintf("workload %q is unsupportable by deep WAS: %s (%s)", e.WorkloadName, e.Reason, e.Message)
}

// WorkloadVirtualPodGroups holds all virtual PodGroup and CompositePodGroup objects
// generated for a Workload, along with mapping functions for virtual pods.
type WorkloadVirtualPodGroups struct {
	PodGroups          []*schedulingv1alpha3.PodGroup
	CompositePodGroups []*schedulingv1alpha3.CompositePodGroup
	LeafPGForPod       func(podSetName string, replicaIdx int) string
}

// virtualGroupName generates a unique deterministic name
func virtualGroupName(prefix, wlName, groupID string) string {
	h := sha1.New()
	h.Write([]byte(wlName + "\n" + groupID))
	hash := hex.EncodeToString(h.Sum(nil))[:hashLength]
	name := fmt.Sprintf("virtual-%s-%s-%s-%s", prefix, wlName, groupID, hash)
	if len(name) > maxPodNameLength {
		return name[:maxPodNameLength]
	}
	return name
}

// newVirtualPG creates a leaf PodGroup with gang count and optional topology constraint.
func newVirtualPG(name, namespace, topology string, count int32, parent *string) *schedulingv1alpha3.PodGroup {
	pg := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: schedulingv1alpha3.PodGroupSpec{
			ParentCompositePodGroupName: parent,
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: count},
			},
		},
	}
	if topology != "" {
		pg.Spec.SchedulingConstraints = &schedulingv1alpha3.PodGroupSchedulingConstraints{
			Topology: []schedulingv1alpha3.TopologyConstraint{{Key: topology}},
		}
	}
	return pg
}

// newVirtualCPG creates a CompositePodGroup with gang groupCount and optional topology constraint.
func newVirtualCPG(name, namespace, topology string, groupCount int32, parent *string) *schedulingv1alpha3.CompositePodGroup {
	cpg := &schedulingv1alpha3.CompositePodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: schedulingv1alpha3.CompositePodGroupSpec{
			ParentCompositePodGroupName: parent,
			SchedulingPolicy: schedulingv1alpha3.CompositePodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.CompositeGangSchedulingPolicy{MinGroupCount: groupCount},
			},
		},
	}
	if topology != "" {
		cpg.Spec.SchedulingConstraints = &schedulingv1alpha3.CompositePodGroupSchedulingConstraints{
			Topology: []schedulingv1alpha3.TopologyConstraint{{Key: topology}},
		}
	}
	return cpg
}

// BuildVirtualPodGroups constructs the virtual PodGroup and CompositePodGroup hierarchy
// for a candidate Workload following WAS limitations and TAS requirements.
func BuildVirtualPodGroups(wl *kueue.Workload) (*WorkloadVirtualPodGroups, error) {
	if wl == nil {
		return nil, fmt.Errorf("workload must not be nil")
	}

	// 1. Reject unsupported preferred topology
	for _, ps := range wl.Spec.PodSets {
		tr := ps.TopologyRequest
		if tr == nil {
			continue
		}
		if tr.Preferred != nil {
			return nil, &ErrUnsupportableWorkload{
				WorkloadName: wl.Name,
				Reason:       ReasonPreferredTopology,
				Message:      fmt.Sprintf("podset %q specifies preferred topology", ps.Name),
			}
		}
	}

	podToLeafPG := make(map[string]map[int]string)
	var allPGs []*schedulingv1alpha3.PodGroup
	var allCPGs []*schedulingv1alpha3.CompositePodGroup

	// 2: Group PodSets sharing podSetGroupName
	type podSetGroup struct {
		name    string
		podSets []kueue.PodSet
	}
	var groups []podSetGroup
	groupMap := make(map[string]int)

	for _, ps := range wl.Spec.PodSets {
		gName := string(ps.Name)
		if ps.TopologyRequest != nil && ps.TopologyRequest.PodSetGroupName != nil {
			gName = *ps.TopologyRequest.PodSetGroupName
		}
		if idx, ok := groupMap[gName]; ok {
			groups[idx].podSets = append(groups[idx].podSets, ps)
		} else {
			groupMap[gName] = len(groups)
			groups = append(groups, podSetGroup{name: gName, podSets: []kueue.PodSet{ps}})
		}
	}

	var topLevelGroupNames []string

	// 3: Build hierarchy bottom-up per PodSet group
	for _, psGroup := range groups {
		var groupChildNames []string

		for _, ps := range psGroup.podSets {
			podSetName := string(ps.Name)
			podToLeafPG[podSetName] = make(map[int]string)
			sliceConstraints := utiltas.PodSetSliceRequiredTopologyConstraints(ps.TopologyRequest)

			if len(sliceConstraints) > 0 {
				var currentLevelNames []string
				// Multi-level slices: innermost leaf PGs, intermediate CPGs
				for lvl := len(sliceConstraints) - 1; lvl >= 0; lvl-- {
					constraint := sliceConstraints[lvl]
					sliceSize := max(1, constraint.Size)
					numSlices := (ps.Count + sliceSize - 1) / sliceSize

					if lvl == len(sliceConstraints)-1 {
						// Innermost level: leaf PodGroups
						for s := int32(0); s < numSlices; s++ {
							pgName := virtualGroupName("pg", wl.Name, fmt.Sprintf("%s-l%d-s%d", podSetName, lvl, s))
							start := s * sliceSize
							end := min((s+1)*sliceSize, ps.Count)
							for p := start; p < end; p++ {
								podToLeafPG[podSetName][int(p)] = pgName
							}
							allPGs = append(allPGs, newVirtualPG(pgName, wl.Namespace, constraint.Topology, end-start, nil))
							currentLevelNames = append(currentLevelNames, pgName)
						}
					} else {
						// Intermediate levels: CompositePodGroups
						var nextLevelNames []string
						childSize := sliceConstraints[lvl+1].Size
						childrenPerParent := max(1, sliceSize/childSize)

						for s := int32(0); s < numSlices; s++ {
							cpgName := virtualGroupName("cpg", wl.Name, fmt.Sprintf("%s-l%d-s%d", podSetName, lvl, s))
							startChild := int(s * childrenPerParent)
							endChild := min(int((s+1)*childrenPerParent), len(currentLevelNames))

							for _, child := range currentLevelNames[startChild:endChild] {
								setParent(allPGs, allCPGs, child, cpgName)
							}
							allCPGs = append(allCPGs, newVirtualCPG(cpgName, wl.Namespace, constraint.Topology, int32(endChild-startChild), nil))
							nextLevelNames = append(nextLevelNames, cpgName)
						}
						currentLevelNames = nextLevelNames
					}
				}
				groupChildNames = append(groupChildNames, currentLevelNames...)

			} else if ps.TopologyRequest != nil && ps.TopologyRequest.Required != nil {
				// Single-level required: leaf PodGroup with topology constraint
				pgName := virtualGroupName("pg", wl.Name, podSetName)
				for p := range int(ps.Count) {
					podToLeafPG[podSetName][p] = pgName
				}
				allPGs = append(allPGs, newVirtualPG(pgName, wl.Namespace, *ps.TopologyRequest.Required, ps.Count, nil))
				groupChildNames = append(groupChildNames, pgName)

			} else {
				// Plain PodSet: leaf PodGroup without topology constraint
				pgName := virtualGroupName("pg", wl.Name, podSetName)
				for p := range int(ps.Count) {
					podToLeafPG[podSetName][p] = pgName
				}
				allPGs = append(allPGs, newVirtualPG(pgName, wl.Namespace, "", ps.Count, nil))
				groupChildNames = append(groupChildNames, pgName)
			}
		}

		// Enclosing CPG for PodSet group if multiple children or slices with required topology
		var groupTopology string
		for _, ps := range psGroup.podSets {
			if ps.TopologyRequest != nil && ps.TopologyRequest.Required != nil {
				groupTopology = *ps.TopologyRequest.Required
				break
			}
		}

		needsEnclosingCPG := len(groupChildNames) > 1 || (len(utiltas.PodSetSliceRequiredTopologyConstraints(psGroup.podSets[0].TopologyRequest)) > 0 && groupTopology != "")
		if !needsEnclosingCPG && len(groupChildNames) == 1 {
			topLevelGroupNames = append(topLevelGroupNames, groupChildNames[0])
		} else if len(groupChildNames) > 0 {
			cpgName := virtualGroupName("cpg", wl.Name, psGroup.name)
			for _, child := range groupChildNames {
				setParent(allPGs, allCPGs, child, cpgName)
			}
			allCPGs = append(allCPGs, newVirtualCPG(cpgName, wl.Namespace, groupTopology, int32(len(groupChildNames)), nil))
			topLevelGroupNames = append(topLevelGroupNames, cpgName)
		}
	}

	// 4: Root CompositePodGroup when multiple top-level groups exist
	if len(topLevelGroupNames) > 1 {
		rootCPGName := virtualGroupName("cpg", wl.Name, "root")
		for _, child := range topLevelGroupNames {
			setParent(allPGs, allCPGs, child, rootCPGName)
		}
		allCPGs = append(allCPGs, newVirtualCPG(rootCPGName, wl.Namespace, "", int32(len(topLevelGroupNames)), nil))
	}

	// 5: Enforce WAS limits
	if err := validateLimits(wl.Name, allPGs, allCPGs); err != nil {
		return nil, err
	}

	return &WorkloadVirtualPodGroups{
		PodGroups:          allPGs,
		CompositePodGroups: allCPGs,
		LeafPGForPod: func(podSetName string, replicaIdx int) string {
			if m, ok := podToLeafPG[podSetName]; ok {
				return m[replicaIdx]
			}
			return ""
		},
	}, nil
}

// setParent sets ParentCompositePodGroupName on the target child group
func setParent(pgs []*schedulingv1alpha3.PodGroup, cpgs []*schedulingv1alpha3.CompositePodGroup, child, parent string) {
	for _, pg := range pgs {
		if pg.Name == child {
			pg.Spec.ParentCompositePodGroupName = ptr.To(parent)
			return
		}
	}
	for _, cpg := range cpgs {
		if cpg.Name == child {
			cpg.Spec.ParentCompositePodGroupName = ptr.To(parent)
			return
		}
	}
}

// validateLimits verifies the limits: max children per CPG (<=8) and hierarchy depth (<=4)
func validateLimits(wlName string, pgs []*schedulingv1alpha3.PodGroup, cpgs []*schedulingv1alpha3.CompositePodGroup) error {
	children := make(map[string]int)
	parentOf := make(map[string]string)

	for _, pg := range pgs {
		if pg.Spec.ParentCompositePodGroupName != nil {
			children[*pg.Spec.ParentCompositePodGroupName]++
		}
	}
	for _, cpg := range cpgs {
		if cpg.Spec.ParentCompositePodGroupName != nil {
			children[*cpg.Spec.ParentCompositePodGroupName]++
			parentOf[cpg.Name] = *cpg.Spec.ParentCompositePodGroupName
		}
	}

	for parent, count := range children {
		if count > maxWASChildrenPerGroup {
			return &ErrUnsupportableWorkload{
				WorkloadName: wlName,
				Reason:       ReasonChildLimitExceeded,
				Message:      fmt.Sprintf("group %q has %d children (max %d)", parent, count, maxWASChildrenPerGroup),
			}
		}
	}

	for _, pg := range pgs {
		depth := 1
		curr := pg.Spec.ParentCompositePodGroupName
		for curr != nil {
			depth++
			if depth > maxWASHierarchyDepth {
				return &ErrUnsupportableWorkload{
					WorkloadName: wlName,
					Reason:       ReasonHierarchyDepthExceeded,
					Message:      fmt.Sprintf("depth %d exceeds maximum of %d", depth, maxWASHierarchyDepth),
				}
			}
			if p, ok := parentOf[*curr]; ok {
				curr = ptr.To(p)
			} else {
				curr = nil
			}
		}
	}
	return nil
}
