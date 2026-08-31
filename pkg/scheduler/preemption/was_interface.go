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

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/fairsharing"
	"sigs.k8s.io/kueue/pkg/workload"
)

// Candidate represents a Kueue workload
type Candidate = *workload.Info

// CandidateList is an ordered list of workloads to consider for preemption
type CandidateList []Candidate

// Strategy defines the protocol between Kueue candidate generation and
// Workload Aware Scheduling (WAS) node placement simulation.
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

type classicalStrategy struct {
	provider   CandidateProvider
	attempts   []bool // allowBorrowing options to try
	currentIdx int
	lastResult bool
}

// NewClassicalStrategy creates a Strategy implementation for Classical preemption
func NewClassicalStrategy(provider CandidateProvider, attemptOptions []bool) Strategy {
	return &classicalStrategy{
		provider: provider,
		attempts: attemptOptions,
	}
}

func (s *classicalStrategy) CandidateSets() iter.Seq[CandidateList] {
	return func(yield func(CandidateList) bool) {
		for s.currentIdx < len(s.attempts) && !s.lastResult {
			idxBeforeYield := s.currentIdx
			allowBorrowing := s.attempts[s.currentIdx]
			candidates := GetCandidatesUpfront(s.provider, allowBorrowing)
			if !yield(candidates) {
				return
			}
			if s.currentIdx == idxBeforeYield {
				s.currentIdx++
			}
		}
	}
}

func (s *classicalStrategy) ReportResult(success bool) {
	s.lastResult = success
	s.currentIdx++
}

func (s *classicalStrategy) ShouldContinue() bool {
	return !s.lastResult && s.currentIdx < len(s.attempts)
}

type fairSharingStrategy struct {
	preemptionCtx *preemptionCtx
	candidates    []*workload.Info
	fsStrategies  []fairsharing.Strategy
	lastResult    bool
}

// NewFairSharingStrategy creates a Strategy implementation for Fair Sharing preemption
func NewFairSharingStrategy(
	preemptionCtx *preemptionCtx,
	candidates []*workload.Info,
	strategies []fairsharing.Strategy,
) Strategy {
	return &fairSharingStrategy{
		preemptionCtx: preemptionCtx,
		candidates:    candidates,
		fsStrategies:  strategies,
	}
}

func (s *fairSharingStrategy) CandidateSets() iter.Seq[CandidateList] {
	return func(yield func(CandidateList) bool) {
		if s.lastResult || len(s.candidates) == 0 || len(s.fsStrategies) == 0 {
			return
		}

		// Phase 1: First FairSharing strategy (Rule S2-a)
		phase1Candidates, retryCandidates := collectFirstFsCandidates(s.preemptionCtx, s.candidates, s.fsStrategies[0])
		if len(phase1Candidates) > 0 {
			if !yield(phase1Candidates) || s.lastResult {
				return
			}
		}

		// Phase 2: Re-evaluation on preemptor DRS drop (if same CQ workloads were present in phase 1)
		if features.Enabled(features.FairSharingReevaluatePreemptionCandidates) &&
			containsCandidateFromCQ(s.preemptionCtx.preemptorCQ.Name, phase1Candidates) &&
			len(retryCandidates) > 0 {
			phase2Candidates, remainingRetries := collectFirstFsCandidates(s.preemptionCtx, retryCandidates, s.fsStrategies[0])
			retryCandidates = remainingRetries
			if len(phase2Candidates) > 0 {
				if !yield(phase2Candidates) || s.lastResult {
					return
				}
			}
		}

		// Phase 3: Second FairSharing strategy (Rule S2-b)
		if len(s.fsStrategies) > 1 && len(retryCandidates) > 0 {
			phase3Candidates := collectSecondFsCandidates(s.preemptionCtx, retryCandidates)
			if len(phase3Candidates) > 0 {
				if !yield(phase3Candidates) || s.lastResult {
					return
				}
			}
		}
	}
}

func (s *fairSharingStrategy) ReportResult(success bool) {
	s.lastResult = success
}

func (s *fairSharingStrategy) ShouldContinue() bool {
	return !s.lastResult
}

func collectFirstFsCandidates(preemptionCtx *preemptionCtx, candidates []*workload.Info, strategy fairsharing.Strategy) (CandidateList, []*workload.Info) {
	ordering := fairsharing.MakeClusterQueueOrdering(preemptionCtx.preemptorCQ, candidates, preemptionCtx.log, preemptionCtx.clock)

	var eligible CandidateList
	var retryCandidates []*workload.Info

	preemptorWithinNominal := features.Enabled(features.FairSharingPreemptWithinNominal) &&
		queueWithinNominalInResourcesNeedingPreemption(preemptionCtx)

	for candCQ := range ordering.Iter() {
		if candCQ.InClusterQueuePreemption() {
			candWl := candCQ.PopWorkload()
			eligible = append(eligible, candWl)
			continue
		}

		if preemptorWithinNominal {
			candWl := candCQ.PopWorkload()
			eligible = append(eligible, candWl)
			continue
		}

		preemptorNewShare, targetOldShare := candCQ.ComputeShares()
		if fsStrategyUnsatisfiable(preemptorNewShare, targetOldShare) {
			for candCQ.HasWorkload() {
				retryCandidates = append(retryCandidates, candCQ.PopWorkload())
			}
			continue
		}

		for candCQ.HasWorkload() {
			candWl := candCQ.PopWorkload()
			targetNewShare := candCQ.ComputeTargetShareAfterRemoval(candWl)
			passed := strategy(preemptorNewShare, targetOldShare, targetNewShare)
			if passed {
				eligible = append(eligible, candWl)
				// One candidate per CQ per round to account for DRS changes
				break
			} else {
				retryCandidates = append(retryCandidates, candWl)
			}
		}
	}
	return eligible, retryCandidates
}

func collectSecondFsCandidates(
	preemptionCtx *preemptionCtx,
	retryCandidates []*workload.Info,
) CandidateList {
	ordering := fairsharing.MakeClusterQueueOrdering(preemptionCtx.preemptorCQ, retryCandidates, preemptionCtx.log, preemptionCtx.clock)
	var eligible CandidateList
	for candCQ := range ordering.Iter() {
		preemptorNewShare, targetOldShare := candCQ.ComputeShares()
		passed := fairsharing.LessThanInitialShare(preemptorNewShare, targetOldShare, fairsharing.TargetNewShare{})
		candWl := candCQ.PopWorkload()
		if passed {
			eligible = append(eligible, candWl)
		}
		ordering.DropQueue(candCQ)
	}
	return eligible
}

func containsCandidateFromCQ(cqName kueue.ClusterQueueReference, list CandidateList) bool {
	for _, cand := range list {
		if cand.ClusterQueue == cqName {
			return true
		}
	}
	return false
}
