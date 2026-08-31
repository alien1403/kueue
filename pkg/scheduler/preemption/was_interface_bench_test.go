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
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/fairsharing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func generateBenchmarkCandidates(count int) []*workload.Info {
	candidates := make([]*workload.Info, count)
	for i := range count {
		candidates[i] = workload.NewInfo(
			utiltestingapi.MakeWorkload(
				fmt.Sprintf("wl-%d", i),
				"ns",
			).Obj(),
		)
	}
	return candidates
}

// BenchmarkCandidateExtractionComparison compares baseline on-demand iterative consumption
// (current Kueue) with the new upfront slice materialization (WAS).
func BenchmarkCandidateExtractionComparison(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mode=baseline_iterative/candidates=%d", count), func(b *testing.B) {
			candidates := generateBenchmarkCandidates(count)
			provider := &mockCandidateProvider{candidates: candidates}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				provider.Reset()
				for c, _ := provider.Next(true); c != nil; c, _ = provider.Next(true) {
					_ = c
				}
			}
		})
		b.Run(fmt.Sprintf("mode=was_upfront_slice/candidates=%d", count), func(b *testing.B) {
			candidates := generateBenchmarkCandidates(count)
			provider := &mockCandidateProvider{candidates: candidates}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				list := GetCandidatesUpfront(provider, true)
				for _, c := range list {
					_ = c
				}
			}
		})
	}
}

// BenchmarkStrategyAttemptComparison compares the baseline multi-attempt loop
// with the new WAS classicalStrategy iterator.
func BenchmarkStrategyAttemptComparison(b *testing.B) {
	for _, count := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("mode=baseline_iterative/candidates=%d", count), func(b *testing.B) {
			candidates := generateBenchmarkCandidates(count)
			provider := &mockCandidateProvider{candidates: candidates}
			attempts := []bool{true, false}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, allowBorrowing := range attempts {
					provider.Reset()
					for c, _ := provider.Next(allowBorrowing); c != nil; c, _ = provider.Next(allowBorrowing) {
						_ = c
					}
				}
			}
		})
		b.Run(fmt.Sprintf("mode=was_strategy_classical_preemption/candidates=%d", count), func(b *testing.B) {
			candidates := generateBenchmarkCandidates(count)
			provider := &mockCandidateProvider{candidates: candidates}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				strategy := NewClassicalStrategy(provider, []bool{true, false})
				for range strategy.CandidateSets() {
					strategy.ReportResult(false)
				}
			}
		})
		b.Run(fmt.Sprintf("mode=was_strategy_fair_sharing/candidates=%d", count), func(b *testing.B) {
			numPerCQ := count / 2
			if numPerCQ < 1 {
				numPerCQ = 1
			}
			fixture := newFsLogFixture(b, logr.Discard(), []fsLogClusterQueue{
				{name: "b", candidates: numPerCQ},
				{name: "c", candidates: count - numPerCQ},
			})
			strategies := []fairsharing.Strategy{
				fairsharing.LessThanOrEqualToFinalShare,
				fairsharing.LessThanInitialShare,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				strategy := NewFairSharingStrategy(fixture.preemptionCtx, fixture.candidates, strategies)
				for range strategy.CandidateSets() {
					strategy.ReportResult(false)
				}
			}
		})
	}
}
