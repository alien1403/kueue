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
	"testing"

	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type mockCandidateProvider struct {
	candidates []*workload.Info
	idx        int
}

func (m *mockCandidateProvider) Reset() {
	m.idx = 0
}

func (m *mockCandidateProvider) Next(allowBorrowing bool) (*workload.Info, string) {
	if m.idx >= len(m.candidates) {
		return nil, ""
	}

	candidate := m.candidates[m.idx]
	m.idx++
	return candidate, "test-reason"
}

type mockStrategy struct {
	candidateSets []CandidateList
	result        bool
}

func (m *mockStrategy) CandidateSets() iter.Seq[CandidateList] {
	return func(yield func(CandidateList) bool) {
		for _, set := range m.candidateSets {
			if !yield(set) {
				return
			}
		}
	}
}

func (m *mockStrategy) ReportResult(success bool) {
	m.result = success
}

func (m *mockStrategy) ShouldContinue() bool {
	return !m.result
}

func TestStrategyInterface(t *testing.T) {
	wl1 := workload.NewInfo(utiltestingapi.MakeWorkload("wl1", "ns").Obj())
	wl2 := workload.NewInfo(utiltestingapi.MakeWorkload("wl2", "ns").Obj())

	strategy := &mockStrategy{
		candidateSets: []CandidateList{
			{wl1},
			{wl1, wl2},
		},
	}

	setsCollected := 0
	for candidateList := range strategy.CandidateSets() {
		setsCollected++
		if setsCollected == 1 && len(candidateList) != 1 {
			t.Fatalf("expected 1 candidate in first set, got %d", len(candidateList))
		}
	}

	if setsCollected != 2 {
		t.Fatalf("expected 2 candidate sets, got %d", setsCollected)
	}

	strategy.ReportResult(true)
	if strategy.ShouldContinue() {
		t.Fatalf("expected strategy to stop after successful preemption")
	}
}

func TestGetCandidatesUpfront(t *testing.T) {
	wl1 := workload.NewInfo(utiltestingapi.MakeWorkload("wl1", "ns").Obj())
	wl2 := workload.NewInfo(utiltestingapi.MakeWorkload("wl2", "ns").Obj())

	provider := &mockCandidateProvider{
		candidates: []*workload.Info{wl1, wl2},
	}

	list := GetCandidatesUpfront(provider, true)
	if len(list) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(list))
	}

	if list[0].Obj.Name != "wl1" || list[1].Obj.Name != "wl2" {
		t.Fatalf("unexpected candidate order")
	}
}

func TestClassicalStrategy(t *testing.T) {
	wl1 := workload.NewInfo(utiltestingapi.MakeWorkload("wl1", "ns").Obj())
	wl2 := workload.NewInfo(utiltestingapi.MakeWorkload("wl2", "ns").Obj())

	provider := &mockCandidateProvider{
		candidates: []*workload.Info{wl1, wl2},
	}

	// Classical strategy trying allowBorrowing without preemption first
	strategy := NewClassicalStrategy(provider, []bool{true, false})

	attemptsExecuted := 0
	for candidateList := range strategy.CandidateSets() {
		attemptsExecuted++
		if len(candidateList) != 2 {
			t.Fatalf("expected 2 candidates, got %d", len(candidateList))
		}
		// first attempt fails, second attempt succeeds
		if attemptsExecuted == 1 {
			strategy.ReportResult(false)
		} else {
			strategy.ReportResult(true)
		}
	}

	if attemptsExecuted != 2 {
		t.Fatalf("expected 2 attempts, got %d", attemptsExecuted)
	}

	if strategy.ShouldContinue() {
		t.Fatalf("expected strategy to stop after successful preemption")
	}
}
