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

package preemption

import (
	"iter"

	"sigs.k8s.io/kueue/pkg/workload"
)

// Candidate represents a Kueue workload
type Candidate = *workload.Info

// CandidateList is an ordered list of workloads to consider for preemption
type CandidateList []Candidate

// Original interface proposal from the WAS sync
type Strategy interface {
	// CandidateSets returns an iterator of candidate lists
	CandidateSets() iter.Seq[CandidateList]
	// ReportResult tells Kueue if WAS successfully preempted a candidate set
	ReportResult(success bool)
	// ShouldContinue returns true if WAS should try the next strategy
	ShouldContinue() bool
}

// CandidateProvider generates candidate workloads for preemption
type CandidateProvider interface {
	Reset()
	Next(allowBorrowing bool) (candidate *workload.Info, reason string)
}

// GetCandidatesUpfront collects all preemption candidates into a slice upfront
func GetCandidatesUpfront(provider CandidateProvider, allowBorrowing bool) CandidateList {
	provider.Reset()
	var list CandidateList
	for candidate, _ := provider.Next(allowBorrowing); candidate != nil; candidate, _ = provider.Next(allowBorrowing) {
		list = append(list, candidate)
	}
	return list
}
