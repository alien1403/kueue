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

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// BuildVirtualPodGroup creates one single gang PodGroup for the entire Workload.
func BuildVirtualPodGroup(wl *kueue.Workload) (*schedulingv1alpha3.PodGroup, error) {
	if wl == nil {
		return nil, fmt.Errorf("workload must not be nil")
	}

	var totalPods int32
	for _, ps := range wl.Spec.PodSets {
		totalPods += ps.Count
	}

	h := sha1.New()
	h.Write([]byte(wl.Name))
	hash := hex.EncodeToString(h.Sum(nil))[:hashLength]
	name := fmt.Sprintf("virtual-pg-%s-%s", wl.Name, hash)
	if len(name) > maxPodNameLength {
		name = name[:maxPodNameLength]
	}

	return &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: wl.Namespace,
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.GangSchedulingPolicy{
					MinCount: totalPods,
				},
			},
		},
	}, nil
}
