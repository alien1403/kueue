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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

func TestBuildVirtualPodGroup(t *testing.T) {
	cases := map[string]struct {
		workload     *kueue.Workload
		wantMinCount int32
		wantErr      bool
	}{
		"nil workload": {
			workload: nil,
			wantErr:  true,
		},
		"single podset workload": {
			workload: utiltesting.MakeWorkload("wl-single", "default").
				PodSets(*utiltesting.MakePodSet("main", 4).Obj()).
				Obj(),
			wantMinCount: 4,
		},
		"multi podset workload": {
			workload: utiltesting.MakeWorkload("wl-multi", "default").
				PodSets(
					*utiltesting.MakePodSet("driver", 1).Obj(),
					*utiltesting.MakePodSet("workers", 8).Obj(),
				).
				Obj(),
			wantMinCount: 9,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pg, err := BuildVirtualPodGroup(tc.workload)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected error: %v, wantErr: %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			if pg == nil {
				t.Fatal("expected non-nil PodGroup")
			}
			if pg.Namespace != tc.workload.Namespace {
				t.Errorf("got namespace %q, want %q", pg.Namespace, tc.workload.Namespace)
			}
			if !strings.HasPrefix(pg.Name, "virtual-pg-"+tc.workload.Name) {
				t.Errorf("got name %q, want prefix 'virtual-pg-%s'", pg.Name, tc.workload.Name)
			}
			if pg.Spec.SchedulingPolicy.Gang == nil {
				t.Fatal("expected non-nil gang policy")
			}
			if diff := cmp.Diff(tc.wantMinCount, pg.Spec.SchedulingPolicy.Gang.MinCount); diff != "" {
				t.Errorf("unexpected minCount (-want,+got):\n%s", diff)
			}
			if pg.Spec.SchedulingConstraints != nil {
				t.Errorf("expected nil scheduling constraints, got: %+v", pg.Spec.SchedulingConstraints)
			}
		})
	}
}
