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

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/cache/scheduler/was"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

const (
	detBlockLabel = "cloud.provider.com/topology-block"
	detRackLabel  = "cloud.provider.com/topology-rack"
	detFlavorName = "tas-flavor"
	detTopoName   = "default-topology"
	detCQName     = "cq-0-0-cohort-0"
	detLQName     = "lq-0-0-cohort-0"
	detNamespace  = "default"
)

type deterministicPreemptionFixture struct {
	topology    *kueue.Topology
	flavor      *kueue.ResourceFlavor
	cq          *kueue.ClusterQueue
	lq          *kueue.LocalQueue
	nodes       []corev1.Node
	admittedWls []kueue.Workload
	incomingWl  *kueue.Workload
}

// buildDeterministicPreemptionFixture constructs the exact 24-node (4 racks x 6 nodes, 8 CPU/node = 192 CPU),
// 1-ClusterQueue (160 CPU nominalQuota) topology and pre-admitted candidate workloads in round-robin rack order.
//
// Each small workload requests 2 pods x 100m = 0.2 CPU (priority 50) and is placed on a single node within
// rack (i % 4), with strictly increasing QuotaReserved timestamps (t0 + i*1ms) so CandidatesOrdering is
// 100% deterministic and identical between Classic Kueue and DeepWAS.
func buildDeterministicPreemptionFixture(numSmallCandidates int, numMediumAdmitted int, packNodesWithinRack bool) deterministicPreemptionFixture {
	return buildDeterministicPreemptionFixtureWithPodCPU(numSmallCandidates, 100, numMediumAdmitted, packNodesWithinRack)
}

func buildDeterministicPreemptionFixtureWithPodCPU(numSmallCandidates int, podMilliCPU int, numMediumAdmitted int, packNodesWithinRack bool) deterministicPreemptionFixture {
	const (
		numRacks     = 4
		nodesPerRack = 6
		numNodes     = numRacks * nodesPerRack
	)
	wlMilliCPU := 2 * podMilliCPU

	nodes := make([]corev1.Node, 0, numNodes)
	rackNodes := make([][]string, numRacks)
	nodeUsedMilliCPU := make([][]int, numRacks)
	for r := range numRacks {
		nodeUsedMilliCPU[r] = make([]int, nodesPerRack)
		for n := range nodesPerRack {
			host := fmt.Sprintf("node-0-%d-%d", r, n)
			rack := fmt.Sprintf("rack-%d", r)
			rackNodes[r] = append(rackNodes[r], host)
			node := *testingnode.MakeNode(host).
				Label(detBlockLabel, "block-0").
				Label(detRackLabel, rack).
				Label(corev1.LabelHostname, host).
				Label("tas-node-group", "true").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("8"),
					corev1.ResourceMemory: resource.MustParse("64Gi"),
					corev1.ResourcePods:   resource.MustParse("1000"),
				}).
				Ready().
				Obj()
			nodes = append(nodes, node)
		}
	}

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	admittedWls := make([]kueue.Workload, 0, numSmallCandidates+numMediumAdmitted)

	// Optional medium workloads (priority 100, 4 pods x 2000m = 8 CPU each), placed on node-0 (4 pods = 8 CPU)
	// of rack (m % 4) to reproduce states where higher-priority workloads already occupy part of the CQ quota
	// (e.g., 680 small = 136 CPU + 2 medium = 16 CPU -> 152/160 CPU used, 8 CPU free in CQ).
	for m := range numMediumAdmitted {
		r := m % numRacks
		admitTime := baseTime.Add(time.Duration(m) * time.Millisecond)
		nodeUsedMilliCPU[r][0] += 8000
		ta := utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{rackNodes[r][0]}, 4).Obj()).
			Obj()
		ps := utiltestingapi.MakePodSet("main", 4).
			Request(corev1.ResourceCPU, "2000m").
			RequiredTopologyRequest(detRackLabel).
			Obj()
		psa := utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceCPU, detFlavorName, "8").
			Count(4).
			TopologyAssignment(ta).
			Obj()
		name := fmt.Sprintf("medium-required-rack-%03d", m)
		wl := utiltestingapi.MakeWorkload(name, detNamespace).
			UID(types.UID(name)).
			Queue(detLQName).
			Priority(100).
			PodSets(*ps).
			ReserveQuotaAt(utiltestingapi.MakeAdmission(detCQName).PodSets(psa).Obj(), admitTime).
			AdmittedAt(true, admitTime).
			Obj()
		admittedWls = append(admittedWls, *wl)
	}

	// Small candidate workloads (priority 50, 2 pods x podMilliCPU each), interleaved across the 4 racks.
	rackRRIdx := make([]int, numRacks)
	podCPUReqStr := fmt.Sprintf("%dm", podMilliCPU)
	wlCPUAssignStr := fmt.Sprintf("%dm", wlMilliCPU)
	for i := range numSmallCandidates {
		r := i % numRacks
		nodeInRack := -1
		if packNodesWithinRack {
			for n := range nodesPerRack {
				if nodeUsedMilliCPU[r][n]+wlMilliCPU <= 8000 {
					nodeInRack = n
					break
				}
			}
		} else {
			for range nodesPerRack {
				candNode := rackRRIdx[r] % nodesPerRack
				rackRRIdx[r]++
				if nodeUsedMilliCPU[r][candNode]+wlMilliCPU <= 8000 {
					nodeInRack = candNode
					break
				}
			}
		}
		if nodeInRack < 0 {
			panic(fmt.Sprintf("no node with %dm free CPU in rack %d for small workload %d", wlMilliCPU, r, i))
		}
		nodeUsedMilliCPU[r][nodeInRack] += wlMilliCPU
		host := rackNodes[r][nodeInRack]
		admitTime := baseTime.Add(time.Duration(1000+i) * time.Millisecond)

		ta := utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{host}, 2).Obj()).
			Obj()
		ps := utiltestingapi.MakePodSet("main", 2).
			Request(corev1.ResourceCPU, podCPUReqStr).
			RequiredTopologyRequest(detRackLabel).
			Obj()
		psa := utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceCPU, detFlavorName, wlCPUAssignStr).
			Count(2).
			TopologyAssignment(ta).
			Obj()
		name := fmt.Sprintf("small-cand-%05d", i)
		wl := utiltestingapi.MakeWorkload(name, detNamespace).
			UID(types.UID(name)).
			Queue(detLQName).
			Priority(50).
			PodSets(*ps).
			ReserveQuotaAt(utiltestingapi.MakeAdmission(detCQName).PodSets(psa).Obj(), admitTime).
			AdmittedAt(true, admitTime).
			Obj()
		admittedWls = append(admittedWls, *wl)
	}

	// Incoming high-priority large workload: 8 pods x 2500m = 20 CPU, priority 200, required on 1 rack.
	psIncoming := utiltestingapi.MakePodSet("main", 8).
		Request(corev1.ResourceCPU, "2500m").
		RequiredTopologyRequest(detRackLabel).
		Obj()
	incomingWl := utiltestingapi.MakeWorkload("large-required-rack-preemptor", detNamespace).
		UID("large-required-rack-preemptor").
		Queue(detLQName).
		Priority(200).
		PodSets(*psIncoming).
		Obj()

	topology := utiltestingapi.MakeTopology(detTopoName).
		Levels(detBlockLabel, detRackLabel, corev1.LabelHostname).
		Obj()
	flavor := utiltestingapi.MakeResourceFlavor(detFlavorName).
		NodeLabel("tas-node-group", "true").
		TopologyName(detTopoName).
		Obj()
	fq := utiltestingapi.MakeFlavorQuotas(detFlavorName).
		Resource(corev1.ResourceCPU, "160").
		Obj()
	cq := utiltestingapi.MakeClusterQueue(detCQName).
		Preemption(kueue.ClusterQueuePreemption{
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			ReclaimWithinCohort: kueue.PreemptionPolicyNever,
		}).
		ResourceGroup(*fq).
		Obj()
	lq := utiltestingapi.MakeLocalQueue(detLQName, detNamespace).
		ClusterQueue(detCQName).
		Obj()

	return deterministicPreemptionFixture{
		topology:    topology,
		flavor:      flavor,
		cq:          cq,
		lq:          lq,
		nodes:       nodes,
		admittedWls: admittedWls,
		incomingWl:  incomingWl,
	}
}

func setDeterministicFeatureGates(tb testing.TB, enableDeepWAS bool) {
	tb.Helper()
	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
		string(features.TopologyAwareScheduling):         true,
		string(features.SchedulerLibraryIntegration):     enableDeepWAS,
		string(features.SchedulerLibraryDeepIntegration): enableDeepWAS,
		"GenericWorkload":                                enableDeepWAS,
		"CompositePodGroup":                              enableDeepWAS,
		"TopologyAwareWorkloadScheduling":                enableDeepWAS,
	}); err != nil {
		tb.Fatalf("setting feature gates: %v", err)
	}
	tb.Cleanup(func() {
		_ = utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.SchedulerLibraryDeepIntegration): false,
			string(features.SchedulerLibraryIntegration):     false,
		})
	})
}

type deterministicCycleMetrics struct {
	snapshotDuration   time.Duration
	nominationDuration time.Duration
	decisionDuration   time.Duration
	preemptedCount     int
	admitted           bool
}

func runSinglePreemptionCycle(t testing.TB, fix deterministicPreemptionFixture, enableDeepWAS bool) deterministicCycleMetrics {
	t.Helper()
	setDeterministicFeatureGates(t, enableDeepWAS)

	log := logr.Discard()
	ctx := ctrl.LoggerInto(t.Context(), log)

	objs := []client.Object{
		utiltesting.MakeNamespaceWrapper(detNamespace).Obj(),
		fix.topology,
	}
	for i := range fix.nodes {
		objs = append(objs, &fix.nodes[i])
	}

	cb := utiltesting.NewClientBuilder().
		WithObjects(objs...).
		WithLists(
			&kueue.WorkloadList{Items: fix.admittedWls},
			&kueue.LocalQueueList{Items: []kueue.LocalQueue{*fix.lq}},
			&kueue.ClusterQueueList{Items: []kueue.ClusterQueue{*fix.cq}},
		).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, client client.Client, subResourceName string, applyConf runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				return nil
			},
			SubResourcePatch: func(ctx context.Context, client client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				return nil
			},
		}).
		WithIndex(&corev1.LimitRange{}, indexer.LimitRangeHasContainerOrPodType, indexer.IndexLimitRangeHasContainerOrPodType)
	cl := cb.Build()
	recorder := &utiltesting.EventRecorder{}

	var cacheOpts []schdcache.Option
	if enableDeepWAS {
		simFactory, err := was.NewWASSimulatorFactory(ctx, nil)
		if err != nil {
			t.Fatalf("initializing WAS simulator factory: %v", err)
		}
		cacheOpts = append(cacheOpts, schdcache.WithSimulatorFactory(simFactory))
	}

	cqCache := schdcache.New(cl, cacheOpts...)
	expStore := preemptexpectations.New()
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithPreemptionExpectations(expStore))

	cqCache.AddOrUpdateTopology(log, fix.topology)
	cqCache.AddOrUpdateResourceFlavor(log, fix.flavor)
	if err := cqCache.AddClusterQueue(ctx, fix.cq); err != nil {
		t.Fatalf("adding ClusterQueue to cqCache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, fix.cq); err != nil {
		t.Fatalf("adding ClusterQueue to qManager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, fix.lq); err != nil {
		t.Fatalf("adding LocalQueue to qManager: %v", err)
	}
	for i := range fix.nodes {
		cqCache.TASCache().SyncNode(&fix.nodes[i])
	}
	for i := range fix.admittedWls {
		if !cqCache.AddOrUpdateWorkload(ctx, log, &fix.admittedWls[i]) {
			t.Fatalf("adding admitted workload %s to cqCache", fix.admittedWls[i].Name)
		}
	}

	incomingCopy := fix.incomingWl.DeepCopy()
	if err := qManager.AddOrUpdateWorkload(ctx, log, incomingCopy); err != nil {
		t.Fatalf("adding incoming workload to qManager: %v", err)
	}

	sched := New(qManager, cqCache, cl, recorder, WithPreemptionExpectations(expStore))

	// Execute the exact two in-memory phases of scheduler.schedule(ctx):
	// 1. Pop heads & take Snapshot
	heads := sched.queues.Heads(ctx)
	if len(heads) != 1 {
		t.Fatalf("expected 1 head workload, got %d", len(heads))
	}

	tSnapStart := time.Now()
	snapshot, err := sched.cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	snapDur := time.Since(tSnapStart)

	// 2. Nominate (runs AssignAndNominate + Preemptor.GetTargets)
	tNomStart := time.Now()
	entries, inadmissible := sched.nominate(ctx, heads, snapshot)
	nomDur := time.Since(tNomStart)

	if len(inadmissible) > 0 || len(entries) != 1 {
		t.Fatalf("expected 1 nominated entry and 0 inadmissible, got entries=%d inadmissible=%d", len(entries), len(inadmissible))
	}
	e := entries[0]
	if e.assignment.RepresentativeMode() != flavorassigner.Preempt {
		t.Fatalf("expected RepresentativeMode() == Preempt, got %v (status=%v)", e.assignment.RepresentativeMode(), e.status)
	}

	return deterministicCycleMetrics{
		snapshotDuration:   snapDur,
		nominationDuration: nomDur,
		decisionDuration:   snapDur + nomDur,
		preemptedCount:     len(e.preemptionTargets),
		admitted:           len(e.preemptionTargets) > 0,
	}
}

// buildAllCandidatesVictims24NodesFixture constructs a 24-node (4 racks x 6 nodes, 8 CPU/node = 192 CPU total) cluster
// filled by numSmallCandidates small workloads (2 pods x podMilliCPU each), and a single incoming preemptor requesting
// 48 pods x 4000m = 192 CPU (requiring 100% of all 24 nodes in the cluster).
//
// Therefore, ALL numSmallCandidates workloads currently in the cluster MUST be preempted (wantVictims = numSmallCandidates)
// in order for the incoming preemptor to fit on the cluster's nodes.
func buildAllCandidatesVictims24NodesFixture(numSmallCandidates int, podMilliCPU int, cqNominalQuotaCPU int) deterministicPreemptionFixture {
	const (
		numRacks     = 4
		nodesPerRack = 6
		numNodes     = numRacks * nodesPerRack // 24 nodes x 8 CPU = 192 CPU
	)
	wlMilliCPU := 2 * podMilliCPU

	nodes := make([]corev1.Node, 0, numNodes)
	rackNodes := make([][]string, numRacks)
	nodeUsedMilliCPU := make([][]int, numRacks)
	for r := range numRacks {
		nodeUsedMilliCPU[r] = make([]int, nodesPerRack)
		for n := range nodesPerRack {
			host := fmt.Sprintf("node-0-%d-%d", r, n)
			rack := fmt.Sprintf("rack-%d", r)
			rackNodes[r] = append(rackNodes[r], host)
			node := *testingnode.MakeNode(host).
				Label(detBlockLabel, "block-0").
				Label(detRackLabel, rack).
				Label(corev1.LabelHostname, host).
				Label("tas-node-group", "true").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("8"),
					corev1.ResourceMemory: resource.MustParse("64Gi"),
					corev1.ResourcePods:   resource.MustParse("1000"),
				}).
				Ready().
				Obj()
			nodes = append(nodes, node)
		}
	}

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	admittedWls := make([]kueue.Workload, 0, numSmallCandidates)
	rackRRIdx := make([]int, numRacks)
	podCPUReqStr := fmt.Sprintf("%dm", podMilliCPU)
	wlCPUAssignStr := fmt.Sprintf("%dm", wlMilliCPU)
	for i := range numSmallCandidates {
		r := i % numRacks
		candNode := rackRRIdx[r] % nodesPerRack
		rackRRIdx[r]++
		nodeUsedMilliCPU[r][candNode] += wlMilliCPU
		host := rackNodes[r][candNode]
		admitTime := baseTime.Add(time.Duration(1000+i) * time.Millisecond)

		ta := utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{host}, 2).Obj()).
			Obj()
		ps := utiltestingapi.MakePodSet("main", 2).
			Request(corev1.ResourceCPU, podCPUReqStr).
			RequiredTopologyRequest(detRackLabel).
			Obj()
		psa := utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceCPU, detFlavorName, wlCPUAssignStr).
			Count(2).
			TopologyAssignment(ta).
			Obj()
		name := fmt.Sprintf("small-cand-%05d", i)
		wl := utiltestingapi.MakeWorkload(name, detNamespace).
			UID(types.UID(name)).
			Queue(detLQName).
			Priority(50).
			PodSets(*ps).
			ReserveQuotaAt(utiltestingapi.MakeAdmission(detCQName).PodSets(psa).Obj(), admitTime).
			AdmittedAt(true, admitTime).
			Obj()
		admittedWls = append(admittedWls, *wl)
	}

	// Incoming preemptor requests 48 pods x 4000m = 192 CPU (2 pods x 4.0 CPU per 8-CPU node across all 24 nodes).
	psIncoming := utiltestingapi.MakePodSet("main", 48).
		Request(corev1.ResourceCPU, "4000m").
		RequiredTopologyRequest(detBlockLabel).
		Obj()
	incomingWl := utiltestingapi.MakeWorkload("all-cluster-preemptor", detNamespace).
		UID("all-cluster-preemptor").
		Queue(detLQName).
		Priority(200).
		PodSets(*psIncoming).
		Obj()

	topology := utiltestingapi.MakeTopology(detTopoName).
		Levels(detBlockLabel, detRackLabel, corev1.LabelHostname).
		Obj()
	flavor := utiltestingapi.MakeResourceFlavor(detFlavorName).
		NodeLabel("tas-node-group", "true").
		TopologyName(detTopoName).
		Obj()
	fq := utiltestingapi.MakeFlavorQuotas(detFlavorName).
		Resource(corev1.ResourceCPU, fmt.Sprintf("%d", cqNominalQuotaCPU)).
		Obj()
	cq := utiltestingapi.MakeClusterQueue(detCQName).
		Preemption(kueue.ClusterQueuePreemption{
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			ReclaimWithinCohort: kueue.PreemptionPolicyNever,
		}).
		ResourceGroup(*fq).
		Obj()
	lq := utiltestingapi.MakeLocalQueue(detLQName, detNamespace).
		ClusterQueue(detCQName).
		Obj()

	return deterministicPreemptionFixture{
		topology:    topology,
		flavor:      flavor,
		cq:          cq,
		lq:          lq,
		nodes:       nodes,
		admittedWls: admittedWls,
		incomingWl:  incomingWl,
	}
}

// buildAllCandidatesVictims2048NodesFixture constructs a large-scale 2,048-node (4 racks x 512 nodes,
// 64 CPU/node = 131,072 CPU total, standard 110 pods/node limit) cluster filled by numSmallCandidates
// workloads (2 pods x podMilliCPU each), and a single incoming preemptor requesting 2,048 pods x 64 CPU = 131,072 CPU
// (requiring 100% of all 2,048 nodes in the cluster).
func buildAllCandidatesVictims2048NodesFixture(numSmallCandidates int, podMilliCPU int, cqNominalQuotaCPU int) deterministicPreemptionFixture {
	const (
		numRacks     = 4
		nodesPerRack = 512
		numNodes     = numRacks * nodesPerRack // 2048 nodes x 64 CPU = 131,072 CPU
	)
	wlMilliCPU := 2 * podMilliCPU

	nodes := make([]corev1.Node, 0, numNodes)
	rackNodes := make([][]string, numRacks)
	nodeUsedMilliCPU := make([][]int, numRacks)
	for r := range numRacks {
		nodeUsedMilliCPU[r] = make([]int, nodesPerRack)
		for n := range nodesPerRack {
			host := fmt.Sprintf("node-0-%d-%d", r, n)
			rack := fmt.Sprintf("rack-%d", r)
			rackNodes[r] = append(rackNodes[r], host)
			node := *testingnode.MakeNode(host).
				Label(detBlockLabel, "block-0").
				Label(detRackLabel, rack).
				Label(corev1.LabelHostname, host).
				Label("tas-node-group", "true").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("64"),
					corev1.ResourceMemory: resource.MustParse("512Gi"),
					corev1.ResourcePods:   resource.MustParse("110"),
				}).
				Ready().
				Obj()
			nodes = append(nodes, node)
		}
	}

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	admittedWls := make([]kueue.Workload, 0, numSmallCandidates)
	rackRRIdx := make([]int, numRacks)
	podCPUReqStr := fmt.Sprintf("%dm", podMilliCPU)
	wlCPUAssignStr := fmt.Sprintf("%dm", wlMilliCPU)
	for i := range numSmallCandidates {
		r := i % numRacks
		candNode := rackRRIdx[r] % nodesPerRack
		rackRRIdx[r]++
		nodeUsedMilliCPU[r][candNode] += wlMilliCPU
		host := rackNodes[r][candNode]
		admitTime := baseTime.Add(time.Duration(1000+i) * time.Millisecond)

		ta := utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{host}, 2).Obj()).
			Obj()
		ps := utiltestingapi.MakePodSet("main", 2).
			Request(corev1.ResourceCPU, podCPUReqStr).
			RequiredTopologyRequest(detRackLabel).
			Obj()
		psa := utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceCPU, detFlavorName, wlCPUAssignStr).
			Count(2).
			TopologyAssignment(ta).
			Obj()
		name := fmt.Sprintf("small-cand-%05d", i)
		wl := utiltestingapi.MakeWorkload(name, detNamespace).
			UID(types.UID(name)).
			Queue(detLQName).
			Priority(50).
			PodSets(*ps).
			ReserveQuotaAt(utiltestingapi.MakeAdmission(detCQName).PodSets(psa).Obj(), admitTime).
			AdmittedAt(true, admitTime).
			Obj()
		admittedWls = append(admittedWls, *wl)
	}

	// Incoming preemptor requests 2048 pods x 64 CPU = 131,072 CPU (1 pod x 64 CPU on every single one of the 2,048 nodes).
	// Since 2048 x 64 CPU = 131,072 CPU = 100% of the 2,048 nodes' capacity, if even a single small workload
	// remains on ANY node, that node has < 64 CPU free (fitting 0 pods of 64 CPU),
	// so ALL numSmallCandidates workloads in the cluster MUST be preempted (wantVictims = numSmallCandidates)
	// for the preemptor to fit!
	psIncoming := utiltestingapi.MakePodSet("main", 2048).
		Request(corev1.ResourceCPU, "64").
		RequiredTopologyRequest(detBlockLabel).
		Obj()
	incomingWl := utiltestingapi.MakeWorkload("all-cluster-preemptor", detNamespace).
		UID("all-cluster-preemptor").
		Queue(detLQName).
		Priority(200).
		PodSets(*psIncoming).
		Obj()

	topology := utiltestingapi.MakeTopology(detTopoName).
		Levels(detBlockLabel, detRackLabel, corev1.LabelHostname).
		Obj()
	flavor := utiltestingapi.MakeResourceFlavor(detFlavorName).
		NodeLabel("tas-node-group", "true").
		TopologyName(detTopoName).
		Obj()
	fq := utiltestingapi.MakeFlavorQuotas(detFlavorName).
		Resource(corev1.ResourceCPU, fmt.Sprintf("%d", cqNominalQuotaCPU)).
		Obj()
	cq := utiltestingapi.MakeClusterQueue(detCQName).
		Preemption(kueue.ClusterQueuePreemption{
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			ReclaimWithinCohort: kueue.PreemptionPolicyNever,
		}).
		ResourceGroup(*fq).
		Obj()
	lq := utiltestingapi.MakeLocalQueue(detLQName, detNamespace).
		ClusterQueue(detCQName).
		Obj()

	return deterministicPreemptionFixture{
		topology:    topology,
		flavor:      flavor,
		cq:          cq,
		lq:          lq,
		nodes:       nodes,
		admittedWls: admittedWls,
		incomingWl:  incomingWl,
	}
}

func TestDeterministicTASPreemptionCycle(t *testing.T) {
	cases := []struct {
		name        string
		fix         deterministicPreemptionFixture
		wantVictims int
	}{
		// --- 24-node (4 racks x 6 nodes, 8 CPU/node = 192 CPU) Single-Rack Preemptor ---
		{
			name:        "24Nodes_Cycle812_NodePackedWithinRack_680Small_2Med_60Victims",
			fix:         buildDeterministicPreemptionFixture(680, 2, true),
			wantVictims: 60,
		},
		{
			name:        "24Nodes_Cycle812_SpreadAcrossNodes_680Small_2Med_60Victims",
			fix:         buildDeterministicPreemptionFixture(680, 2, false),
			wantVictims: 60,
		},
		{
			name:        "24Nodes_FullCQ_NodePackedWithinRack_800Small_0Med_100Victims",
			fix:         buildDeterministicPreemptionFixture(800, 0, true),
			wantVictims: 100,
		},
		{
			name:        "24Nodes_FullCQ_SpreadAcrossNodes_800Small_0Med_100Victims",
			fix:         buildDeterministicPreemptionFixture(800, 0, false),
			wantVictims: 100,
		},
		{
			name:        "24Nodes_FullCQ_NodePackedWithinRack_2000Small_0Med_250Victims",
			fix:         buildDeterministicPreemptionFixtureWithPodCPU(2000, 40, 0, true),
			wantVictims: 250,
		},
		{
			name:        "24Nodes_FullCQ_SpreadAcrossNodes_2000Small_0Med_250Victims",
			fix:         buildDeterministicPreemptionFixtureWithPodCPU(2000, 40, 0, false),
			wantVictims: 250,
		},
		{
			name:        "24Nodes_FullCQ_NodePackedWithinRack_10000Small_0Med_1250Victims",
			fix:         buildDeterministicPreemptionFixtureWithPodCPU(10000, 8, 0, true),
			wantVictims: 1250,
		},
		{
			name:        "24Nodes_FullCQ_SpreadAcrossNodes_10000Small_0Med_1250Victims",
			fix:         buildDeterministicPreemptionFixtureWithPodCPU(10000, 8, 0, false),
			wantVictims: 1250,
		},
		// --- 24-node (4 racks x 6 nodes, 8 CPU/node = 192 CPU) Whole-Cluster Preemptor (All Candidates Evicted) ---
		{
			name:        "24Nodes_All800Evicted_ExactQuota192CPU_800Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(800, 100, 192),
			wantVictims: 800,
		},
		{
			name:        "24Nodes_All800Evicted_SpareQuota220CPU_800Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(800, 100, 220),
			wantVictims: 800,
		},
		{
			name:        "24Nodes_All800Evicted_NodeBoundQuota360CPU_800Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(800, 100, 360),
			wantVictims: 800,
		},
		{
			name:        "24Nodes_All2000Evicted_ExactQuota192CPU_2000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(2000, 40, 192),
			wantVictims: 2000,
		},
		{
			name:        "24Nodes_All2000Evicted_SpareQuota220CPU_2000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(2000, 40, 220),
			wantVictims: 2000,
		},
		{
			name:        "24Nodes_All2000Evicted_NodeBoundQuota360CPU_2000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(2000, 40, 360),
			wantVictims: 2000,
		},
		{
			name:        "24Nodes_All10000Evicted_ExactQuota192CPU_10000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(10000, 8, 192),
			wantVictims: 10000,
		},
		{
			name:        "24Nodes_All10000Evicted_SpareQuota220CPU_10000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(10000, 8, 220),
			wantVictims: 10000,
		},
		{
			name:        "24Nodes_All10000Evicted_NodeBoundQuota360CPU_10000Victims",
			fix:         buildAllCandidatesVictims24NodesFixture(10000, 8, 360),
			wantVictims: 10000,
		},
		// --- 2,048-node (4 racks x 512 nodes, 64 CPU/node = 131,072 CPU) Whole-Cluster Preemptor (All Candidates Evicted) ---
		{
			name:        "2048Nodes_All800Evicted_ExactQuota131072CPU_800Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(800, 25000, 131072),
			wantVictims: 800,
		},
		{
			name:        "2048Nodes_All800Evicted_SpareQuota138072CPU_800Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(800, 25000, 138072),
			wantVictims: 800,
		},
		{
			name:        "2048Nodes_All800Evicted_NodeBoundQuota200000CPU_800Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(800, 25000, 200000),
			wantVictims: 800,
		},
		{
			name:        "2048Nodes_All2000Evicted_ExactQuota131072CPU_2000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(2000, 20000, 131072),
			wantVictims: 2000,
		},
		{
			name:        "2048Nodes_All2000Evicted_SpareQuota145072CPU_2000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(2000, 20000, 145072),
			wantVictims: 2000,
		},
		{
			name:        "2048Nodes_All2000Evicted_NodeBoundQuota240000CPU_2000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(2000, 20000, 240000),
			wantVictims: 2000,
		},
		{
			name:        "2048Nodes_All10000Evicted_ExactQuota131072CPU_10000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(10000, 4000, 131072),
			wantVictims: 10000,
		},
		{
			name:        "2048Nodes_All10000Evicted_SpareQuota145072CPU_10000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(10000, 4000, 145072),
			wantVictims: 10000,
		},
		{
			name:        "2048Nodes_All10000Evicted_NodeBoundQuota240000CPU_10000Victims",
			fix:         buildAllCandidatesVictims2048NodesFixture(10000, 4000, 240000),
			wantVictims: 10000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			classic := runSinglePreemptionCycle(t, tc.fix, false)
			deepWAS := runSinglePreemptionCycle(t, tc.fix, true)

			if classic.preemptedCount != tc.wantVictims {
				t.Errorf("Classic preemptedCount = %d, want %d", classic.preemptedCount, tc.wantVictims)
			}
			if deepWAS.preemptedCount != tc.wantVictims {
				t.Errorf("DeepWAS preemptedCount = %d, want %d", deepWAS.preemptedCount, tc.wantVictims)
			}

			snapRatio := float64(deepWAS.snapshotDuration) / float64(classic.snapshotDuration)
			nomRatio := float64(deepWAS.nominationDuration) / float64(classic.nominationDuration)
			decRatio := float64(deepWAS.decisionDuration) / float64(classic.decisionDuration)

			t.Logf("=== %s (admitted=%d, wantVictims=%d) ===",
				tc.name, len(tc.fix.admittedWls), tc.wantVictims)
			t.Logf("  Classic : Snapshot=%8.2fms | Nomination=%8.2fms | Decision=%8.2fms | Victims=%d",
				float64(classic.snapshotDuration.Microseconds())/1000.0,
				float64(classic.nominationDuration.Microseconds())/1000.0,
				float64(classic.decisionDuration.Microseconds())/1000.0,
				classic.preemptedCount)
			t.Logf("  DeepWAS : Snapshot=%8.2fms | Nomination=%8.2fms | Decision=%8.2fms | Victims=%d",
				float64(deepWAS.snapshotDuration.Microseconds())/1000.0,
				float64(deepWAS.nominationDuration.Microseconds())/1000.0,
				float64(deepWAS.decisionDuration.Microseconds())/1000.0,
				deepWAS.preemptedCount)
			t.Logf("  Slowdown: Snapshot=%6.1fx   | Nomination=%6.1fx   | Decision=%6.1fx",
				snapRatio, nomRatio, decRatio)
		})
	}
}

func BenchmarkDeterministicTASPreemptionCycle(b *testing.B) {
	fix := buildDeterministicPreemptionFixture(680, 2, false)
	for _, enableDeepWAS := range []bool{false, true} {
		mode := "Classic"
		if enableDeepWAS {
			mode = "DeepWAS"
		}
		b.Run(mode, func(b *testing.B) {
			setDeterministicFeatureGates(b, enableDeepWAS)
			log := logr.Discard()
			ctx := ctrl.LoggerInto(b.Context(), log)
			var simFactoryOpts []schdcache.Option
			if enableDeepWAS {
				simFactory, err := was.NewWASSimulatorFactory(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				simFactoryOpts = append(simFactoryOpts, schdcache.WithSimulatorFactory(simFactory))
			}

			for b.Loop() {
				b.StopTimer()
				objs := []client.Object{utiltesting.MakeNamespaceWrapper(detNamespace).Obj(), fix.topology}
				for i := range fix.nodes {
					objs = append(objs, &fix.nodes[i])
				}
				cb := utiltesting.NewClientBuilder().
					WithObjects(objs...).
					WithLists(
						&kueue.WorkloadList{Items: fix.admittedWls},
						&kueue.LocalQueueList{Items: []kueue.LocalQueue{*fix.lq}},
						&kueue.ClusterQueueList{Items: []kueue.ClusterQueue{*fix.cq}},
					).
					WithStatusSubresource(&kueue.Workload{}).
					WithIndex(&corev1.LimitRange{}, indexer.LimitRangeHasContainerOrPodType, indexer.IndexLimitRangeHasContainerOrPodType)
				cl := cb.Build()
				recorder := &utiltesting.EventRecorder{}
				cqCache := schdcache.New(cl, simFactoryOpts...)
				expStore := preemptexpectations.New()
				qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithPreemptionExpectations(expStore))
				cqCache.AddOrUpdateTopology(log, fix.topology)
				cqCache.AddOrUpdateResourceFlavor(log, fix.flavor)
				_ = cqCache.AddClusterQueue(ctx, fix.cq)
				_ = qManager.AddClusterQueue(ctx, fix.cq)
				_ = qManager.AddLocalQueue(ctx, fix.lq)
				for i := range fix.nodes {
					cqCache.TASCache().SyncNode(&fix.nodes[i])
				}
				for i := range fix.admittedWls {
					cqCache.AddOrUpdateWorkload(ctx, log, &fix.admittedWls[i])
				}
				_ = qManager.AddOrUpdateWorkload(ctx, log, fix.incomingWl.DeepCopy())
				sched := New(qManager, cqCache, cl, recorder, WithPreemptionExpectations(expStore))
				heads := sched.queues.Heads(ctx)

				b.StartTimer()
				snapshot, err := sched.cache.Snapshot(ctx)
				if err != nil {
					b.Fatal(err)
				}
				entries, _ := sched.nominate(ctx, heads, snapshot)
				b.StopTimer()

				if len(entries) != 1 || len(entries[0].preemptionTargets) != 60 {
					b.Fatalf("unexpected victims: %d", len(entries[0].preemptionTargets))
				}
			}
		})
	}
}
