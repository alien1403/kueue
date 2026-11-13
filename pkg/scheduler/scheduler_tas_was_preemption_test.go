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
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
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
	topology         *kueue.Topology
	flavor           *kueue.ResourceFlavor
	cq               *kueue.ClusterQueue
	lq               *kueue.LocalQueue
	extraCQs         []kueue.ClusterQueue
	extraLQs         []kueue.LocalQueue
	nodes            []corev1.Node
	admittedWls      []kueue.Workload
	incomingWl       *kueue.Workload
	extraIncomingWls []*kueue.Workload
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
		"GenericWorkload":                 enableDeepWAS,
		"CompositePodGroup":               enableDeepWAS,
		"TopologyAwareWorkloadScheduling": enableDeepWAS,
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

type phaseMemStats struct {
	allocBytes uint64
	mallocs    uint64
	numGC      uint32
}

func measureMemStats[T any](fn func() T) (T, phaseMemStats) {
	var before, after goruntime.MemStats
	goruntime.ReadMemStats(&before)
	res := fn()
	goruntime.ReadMemStats(&after)
	return res, phaseMemStats{
		allocBytes: after.TotalAlloc - before.TotalAlloc,
		mallocs:    after.Mallocs - before.Mallocs,
		numGC:      after.NumGC - before.NumGC,
	}
}

type deterministicCycleMetrics struct {
	coldSnapshotDuration time.Duration
	snapshotDuration     time.Duration
	nominationDuration   time.Duration
	decisionDuration     time.Duration
	coldSnapMem          phaseMemStats
	snapMem              phaseMemStats
	nomMem               phaseMemStats
	preemptedCount       int
	admitted             bool
}

type cycleRunOpts struct {
	skipColdSnapshot bool
	cpuProfPath      string
	memProfPath      string
}

func runSinglePreemptionCycle(t testing.TB, fix deterministicPreemptionFixture, enableDeepWAS bool) deterministicCycleMetrics {
	t.Helper()
	return runSinglePreemptionCycleWithOpts(t, fix, enableDeepWAS, cycleRunOpts{})
}

func runSinglePreemptionCycleWithOpts(t testing.TB, fix deterministicPreemptionFixture, enableDeepWAS bool, opts cycleRunOpts) deterministicCycleMetrics {
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

	cqs := make([]kueue.ClusterQueue, 0, 1+len(fix.extraCQs))
	cqs = append(cqs, *fix.cq)
	cqs = append(cqs, fix.extraCQs...)

	lqs := make([]kueue.LocalQueue, 0, 1+len(fix.extraLQs))
	lqs = append(lqs, *fix.lq)
	lqs = append(lqs, fix.extraLQs...)

	cb := utiltesting.NewClientBuilder().
		WithObjects(objs...).
		WithLists(
			&kueue.WorkloadList{Items: fix.admittedWls},
			&kueue.LocalQueueList{Items: lqs},
			&kueue.ClusterQueueList{Items: cqs},
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
	for i := range cqs {
		if err := cqCache.AddClusterQueue(ctx, &cqs[i]); err != nil {
			t.Fatalf("adding ClusterQueue %s to cqCache: %v", cqs[i].Name, err)
		}
		if err := qManager.AddClusterQueue(ctx, &cqs[i]); err != nil {
			t.Fatalf("adding ClusterQueue %s to qManager: %v", cqs[i].Name, err)
		}
	}
	for i := range lqs {
		if err := qManager.AddLocalQueue(ctx, &lqs[i]); err != nil {
			t.Fatalf("adding LocalQueue %s to qManager: %v", lqs[i].Name, err)
		}
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
	for _, extraWl := range fix.extraIncomingWls {
		if err := qManager.AddOrUpdateWorkload(ctx, log, extraWl.DeepCopy()); err != nil {
			t.Fatalf("adding extra incoming workload %s to qManager: %v", extraWl.Name, err)
		}
	}

	sched := New(qManager, cqCache, cl, recorder, WithPreemptionExpectations(expStore))

	// Execute the exact two in-memory phases of scheduler.schedule(ctx):
	// 1. Pop heads & take Snapshot
	heads := sched.queues.Heads(ctx)
	wantHeads := 1 + len(fix.extraIncomingWls)
	if len(heads) != wantHeads {
		t.Fatalf("expected %d head workloads, got %d", wantHeads, len(heads))
	}

	// 1a. Cold Snapshot (Cycle 1, un-cached)
	var coldSnapDur time.Duration
	var coldSnapMem phaseMemStats
	if !opts.skipColdSnapshot {
		var coldErr error
		coldSnapDur, coldSnapMem = measureMemStats(func() time.Duration {
			tStart := time.Now()
			_, coldErr = sched.cache.Snapshot(ctx)
			return time.Since(tStart)
		})
		if coldErr != nil {
			t.Fatalf("cold Snapshot failed: %v", coldErr)
		}
	}

	// 1b. Warm Snapshot (Cycle 2, hits virtual pod cache)
	type snapRes struct {
		snap *schdcache.Snapshot
		dur  time.Duration
		err  error
	}
	sRes, snapMem := measureMemStats(func() snapRes {
		tStart := time.Now()
		s, err := sched.cache.Snapshot(ctx)
		return snapRes{snap: s, dur: time.Since(tStart), err: err}
	})
	if sRes.err != nil {
		t.Fatalf("Snapshot failed: %v", sRes.err)
	}
	snapshot := sRes.snap
	snapDur := sRes.dur

	// 2. Nominate (runs AssignAndNominate + Preemptor.GetTargets)
	var cpuFile *os.File
	if opts.cpuProfPath != "" {
		if err := os.MkdirAll(filepath.Dir(opts.cpuProfPath), 0o755); err != nil {
			t.Fatalf("creating cpuprof dir: %v", err)
		}
		f, err := os.Create(opts.cpuProfPath)
		if err != nil {
			t.Fatalf("creating cpuprof file %s: %v", opts.cpuProfPath, err)
		}
		cpuFile = f
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			t.Fatalf("starting CPU profile: %v", err)
		}
	}

	type nomRes struct {
		entries      []entry
		inadmissible []entry
		dur          time.Duration
	}
	nRes, nomMem := measureMemStats(func() nomRes {
		tStart := time.Now()
		en, inad := sched.nominate(ctx, heads, snapshot)
		return nomRes{entries: en, inadmissible: inad, dur: time.Since(tStart)}
	})

	if cpuFile != nil {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
	}
	if opts.memProfPath != "" {
		if err := os.MkdirAll(filepath.Dir(opts.memProfPath), 0o755); err != nil {
			t.Fatalf("creating memprof dir: %v", err)
		}
		f, err := os.Create(opts.memProfPath)
		if err != nil {
			t.Fatalf("creating memprof file %s: %v", opts.memProfPath, err)
		}
		goruntime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			_ = f.Close()
			t.Fatalf("writing heap profile: %v", err)
		}
		_ = f.Close()
	}

	entries, inadmissible := nRes.entries, nRes.inadmissible
	nomDur := nRes.dur

	if wantHeads == 1 {
		if len(inadmissible) > 0 || len(entries) != 1 {
			t.Fatalf("expected 1 nominated entry and 0 inadmissible, got entries=%d inadmissible=%d", len(entries), len(inadmissible))
		}
		if entries[0].assignment.RepresentativeMode() != flavorassigner.Preempt {
			t.Fatalf("expected RepresentativeMode() == Preempt, got %v (status=%v)", entries[0].assignment.RepresentativeMode(), entries[0].status)
		}
	}

	totalVictims := 0
	for i := range entries {
		totalVictims += len(entries[i].preemptionTargets)
	}

	return deterministicCycleMetrics{
		coldSnapshotDuration: coldSnapDur,
		snapshotDuration:     snapDur,
		nominationDuration:   nomDur,
		decisionDuration:     snapDur + nomDur,
		coldSnapMem:          coldSnapMem,
		snapMem:              snapMem,
		nomMem:               nomMem,
		preemptedCount:       totalVictims,
		admitted:             totalVictims > 0,
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
			t.Logf("  Classic          : Snapshot=%8.2fms | Nomination=%8.2fms | Decision=%8.2fms | Victims=%d",
				float64(classic.snapshotDuration.Microseconds())/1000.0,
				float64(classic.nominationDuration.Microseconds())/1000.0,
				float64(classic.decisionDuration.Microseconds())/1000.0,
				classic.preemptedCount)
			t.Logf("  DeepWAS (Baseline): Snapshot=%8.2fms | Nomination=%8.2fms | Decision=%8.2fms",
				float64(deepWAS.coldSnapshotDuration.Microseconds())/1000.0,
				float64(deepWAS.nominationDuration.Microseconds())/1000.0,
				float64((deepWAS.coldSnapshotDuration+deepWAS.nominationDuration).Microseconds())/1000.0)
			t.Logf("  DeepWAS (Cached)  : Snapshot=%8.2fms | Nomination=%8.2fms | Decision=%8.2fms | Victims=%d",
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
	benchmarkDeterministicTASPreemptionCycleWithFixture(b, buildDeterministicPreemptionFixture(680, 2, false), 60)
}

func BenchmarkDeterministicTASPreemptionCycle2048Nodes(b *testing.B) {
	benchmarkDeterministicTASPreemptionCycleWithFixture(b, buildAllCandidatesVictims2048NodesFixture(800, 25000, 131072), 800)
}

func benchmarkDeterministicTASPreemptionCycleWithFixture(b *testing.B, fix deterministicPreemptionFixture, wantVictims int) {
	modes := []struct {
		name          string
		enableDeepWAS bool
		warmup        bool
	}{
		{name: "Classic", enableDeepWAS: false, warmup: false},
		{name: "DeepWAS-Baseline", enableDeepWAS: true, warmup: false},
		{name: "DeepWAS-Cached", enableDeepWAS: true, warmup: true},
	}
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			setDeterministicFeatureGates(b, m.enableDeepWAS)
			log := logr.Discard()
			ctx := ctrl.LoggerInto(b.Context(), log)
			var sharedSimFactoryOpts []schdcache.Option
			if m.enableDeepWAS && m.warmup {
				simFactory, err := was.NewWASSimulatorFactory(ctx, nil)
				if err != nil {
					b.Fatal(err)
				}
				sharedSimFactoryOpts = append(sharedSimFactoryOpts, schdcache.WithSimulatorFactory(simFactory))
			}

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterSimFactoryOpts := sharedSimFactoryOpts
				if m.enableDeepWAS && !m.warmup {
					simFactory, err := was.NewWASSimulatorFactory(ctx, nil)
					if err != nil {
						b.Fatal(err)
					}
					iterSimFactoryOpts = []schdcache.Option{schdcache.WithSimulatorFactory(simFactory)}
				}
				objs := []client.Object{utiltesting.MakeNamespaceWrapper(detNamespace).Obj(), fix.topology}
				for j := range fix.nodes {
					objs = append(objs, &fix.nodes[j])
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
				cqCache := schdcache.New(cl, iterSimFactoryOpts...)
				expStore := preemptexpectations.New()
				qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithPreemptionExpectations(expStore))
				cqCache.AddOrUpdateTopology(log, fix.topology)
				cqCache.AddOrUpdateResourceFlavor(log, fix.flavor)
				_ = cqCache.AddClusterQueue(ctx, fix.cq)
				_ = qManager.AddClusterQueue(ctx, fix.cq)
				_ = qManager.AddLocalQueue(ctx, fix.lq)
				for j := range fix.nodes {
					cqCache.TASCache().SyncNode(&fix.nodes[j])
				}
				for j := range fix.admittedWls {
					cqCache.AddOrUpdateWorkload(ctx, log, &fix.admittedWls[j])
				}
				_ = qManager.AddOrUpdateWorkload(ctx, log, fix.incomingWl.DeepCopy())
				sched := New(qManager, cqCache, cl, recorder, WithPreemptionExpectations(expStore))
				heads := sched.queues.Heads(ctx)

				if m.warmup {
					// Warm up cache so the timed loop measures steady state.
					if _, err := sched.cache.Snapshot(ctx); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()
				snapshot, err := sched.cache.Snapshot(ctx)
				if err != nil {
					b.Fatal(err)
				}
				entries, _ := sched.nominate(ctx, heads, snapshot)
				b.StopTimer()

				if len(entries) != 1 || len(entries[0].preemptionTargets) != wantVictims {
					b.Fatalf("unexpected victims: %d (want %d)", len(entries[0].preemptionTargets), wantVictims)
				}
			}
		})
	}
}

type GenericFixtureParams struct {
	NumRacks         int
	NodesPerRack     int
	CPUPerNode       int
	NumCandidates    int
	PodsPerWorkload  int
	PodMilliCPU      int
	TargetUtilPct    int    // default 80
	TASMode          string // "required", "balanced"
	QuotaRegime      string // "exact", "spare", "spare_20%", "node_bound"
	PreemptorScope   string // "single_node", "single_rack", "half_cluster", "all_cluster"
	SpreadCandidates bool
	OverlapHeads     bool // if true, uses 2 CQs in a cohort with 2 overlapping heads to exercise committedVictims
}

func buildGenericDeterministicFixture(p GenericFixtureParams) (deterministicPreemptionFixture, int) {
	if p.NumRacks <= 0 {
		p.NumRacks = 4
	}
	if p.NodesPerRack <= 0 {
		p.NodesPerRack = 6
	}
	if p.CPUPerNode <= 0 {
		if p.NumRacks*p.NodesPerRack >= 100 {
			p.CPUPerNode = 64
		} else {
			p.CPUPerNode = 8
		}
	}
	if p.PodsPerWorkload <= 0 {
		p.PodsPerWorkload = 2
	}
	if p.TargetUtilPct <= 0 {
		p.TargetUtilPct = 80
	}
	if p.TASMode == "" {
		p.TASMode = "required"
	}

	numNodes := p.NumRacks * p.NodesPerRack
	totalClusterCPU := numNodes * p.CPUPerNode
	totalClusterMilliCPU := totalClusterCPU * 1000

	podMilliCPU := p.PodMilliCPU
	if podMilliCPU <= 0 {
		targetMilliCPU := (totalClusterMilliCPU * p.TargetUtilPct) / 100
		podMilliCPU = targetMilliCPU / (p.NumCandidates * p.PodsPerWorkload)
		if podMilliCPU < 1 {
			podMilliCPU = 1
		}
	}
	wlMilliCPU := p.PodsPerWorkload * podMilliCPU

	maxPodsPerNode := 110
	if needed := (p.NumCandidates*p.PodsPerWorkload)/numNodes + 100; needed > maxPodsPerNode {
		maxPodsPerNode = needed
	}

	nodes := make([]corev1.Node, 0, numNodes)
	rackNodes := make([][]string, p.NumRacks)
	nodeUsedMilliCPU := make([][]int, p.NumRacks)
	for r := range p.NumRacks {
		nodeUsedMilliCPU[r] = make([]int, p.NodesPerRack)
		for n := range p.NodesPerRack {
			host := fmt.Sprintf("node-0-%d-%d", r, n)
			rack := fmt.Sprintf("rack-%d", r)
			rackNodes[r] = append(rackNodes[r], host)
			node := *testingnode.MakeNode(host).
				Label(detBlockLabel, "block-0").
				Label(detRackLabel, rack).
				Label(corev1.LabelHostname, host).
				Label("tas-node-group", "true").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", p.CPUPerNode)),
					corev1.ResourceMemory: resource.MustParse("512Gi"),
					corev1.ResourcePods:   resource.MustParse(fmt.Sprintf("%d", maxPodsPerNode)),
				}).
				Ready().
				Obj()
			nodes = append(nodes, node)
		}
	}

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	admittedWls := make([]kueue.Workload, 0, p.NumCandidates)
	rackRRIdx := make([]int, p.NumRacks)
	podCPUReqStr := fmt.Sprintf("%dm", podMilliCPU)
	wlCPUAssignStr := fmt.Sprintf("%dm", wlMilliCPU)

	sliceSize := int32(max(1, p.PodsPerWorkload/2))
	for i := range p.NumCandidates {
		r := i % p.NumRacks
		var nodeInRack int
		if p.SpreadCandidates {
			nodeInRack = rackRRIdx[r] % p.NodesPerRack
			rackRRIdx[r]++
		} else {
			nodeCapMilli := p.CPUPerNode * 1000
			found := false
			for n := range p.NodesPerRack {
				if nodeUsedMilliCPU[r][n]+wlMilliCPU <= nodeCapMilli {
					nodeInRack = n
					found = true
					break
				}
			}
			if !found {
				nodeInRack = rackRRIdx[r] % p.NodesPerRack
				rackRRIdx[r]++
			}
		}

		nodeUsedMilliCPU[r][nodeInRack] += wlMilliCPU
		host := rackNodes[r][nodeInRack]
		admitTime := baseTime.Add(time.Duration(1000+i) * time.Millisecond)

		ta := utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{host}, int32(p.PodsPerWorkload)).Obj()).
			Obj()
		psBuilder := utiltestingapi.MakePodSet("main", p.PodsPerWorkload).
			Request(corev1.ResourceCPU, podCPUReqStr)
		if p.TASMode == "balanced" {
			psBuilder = psBuilder.SliceRequiredTopologyRequest(detRackLabel).SliceSizeTopologyRequest(sliceSize)
		} else {
			psBuilder = psBuilder.RequiredTopologyRequest(detRackLabel)
		}
		ps := psBuilder.Obj()
		psa := utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceCPU, detFlavorName, wlCPUAssignStr).
			Count(int32(p.PodsPerWorkload)).
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

	makeIncomingWl := func(name string, lqName kueue.LocalQueueName, scope string) (*kueue.Workload, int) {
		switch scope {
		case "single_node":
			ps := utiltestingapi.MakePodSet("main", 1).
				Request(corev1.ResourceCPU, fmt.Sprintf("%d", p.CPUPerNode)).
				RequiredTopologyRequest(corev1.LabelHostname).
				Obj()
			wl := utiltestingapi.MakeWorkload(name, detNamespace).
				UID(types.UID(name)).
				Queue(lqName).
				Priority(200).
				PodSets(*ps).
				Obj()
			return wl, -1 // depends on node layout
		case "single_rack":
			ps := utiltestingapi.MakePodSet("main", p.NodesPerRack).
				Request(corev1.ResourceCPU, fmt.Sprintf("%d", p.CPUPerNode)).
				RequiredTopologyRequest(detRackLabel).
				Obj()
			wl := utiltestingapi.MakeWorkload(name, detNamespace).
				UID(types.UID(name)).
				Queue(lqName).
				Priority(200).
				PodSets(*ps).
				Obj()
			rack0Candidates := 0
			for i := range p.NumCandidates {
				if i%p.NumRacks == 0 {
					rack0Candidates++
				}
			}
			return wl, rack0Candidates
		case "half_cluster":
			halfNodes := max(1, numNodes/2)
			ps := utiltestingapi.MakePodSet("main", halfNodes).
				Request(corev1.ResourceCPU, fmt.Sprintf("%d", p.CPUPerNode)).
				RequiredTopologyRequest(detBlockLabel).
				Obj()
			wl := utiltestingapi.MakeWorkload(name, detNamespace).
				UID(types.UID(name)).
				Queue(lqName).
				Priority(200).
				PodSets(*ps).
				Obj()
			return wl, -1
		case "all_cluster":
			fallthrough
		default:
			ps := utiltestingapi.MakePodSet("main", numNodes).
				Request(corev1.ResourceCPU, fmt.Sprintf("%d", p.CPUPerNode)).
				RequiredTopologyRequest(detBlockLabel).
				Obj()
			wl := utiltestingapi.MakeWorkload(name, detNamespace).
				UID(types.UID(name)).
				Queue(lqName).
				Priority(200).
				PodSets(*ps).
				Obj()
			return wl, p.NumCandidates
		}
	}

	incomingWl, wantVictims := makeIncomingWl("preemptor-0", detLQName, p.PreemptorScope)

	var cqNominalQuotaCPU int
	switch p.QuotaRegime {
	case "spare", "spare_10%":
		cqNominalQuotaCPU = int(float64(totalClusterCPU) * 1.10)
	case "spare_20%":
		cqNominalQuotaCPU = int(float64(totalClusterCPU) * 1.20)
	case "node_bound":
		cqNominalQuotaCPU = totalClusterCPU * 2
	case "exact":
		fallthrough
	default:
		cqNominalQuotaCPU = totalClusterCPU
	}

	topology := utiltestingapi.MakeTopology(detTopoName).
		Levels(detBlockLabel, detRackLabel, corev1.LabelHostname).
		Obj()
	flavor := utiltestingapi.MakeResourceFlavor(detFlavorName).
		NodeLabel("tas-node-group", "true").
		TopologyName(detTopoName).
		Obj()

	var cq *kueue.ClusterQueue
	var lq *kueue.LocalQueue
	var extraCQs []kueue.ClusterQueue
	var extraLQs []kueue.LocalQueue
	var extraIncomingWls []*kueue.Workload

	if p.OverlapHeads {
		halfQuota := max(1, cqNominalQuotaCPU/2)
		fq0 := utiltestingapi.MakeFlavorQuotas(detFlavorName).
			Resource(corev1.ResourceCPU, fmt.Sprintf("%d", halfQuota)).
			Obj()
		fq1 := utiltestingapi.MakeFlavorQuotas(detFlavorName).
			Resource(corev1.ResourceCPU, fmt.Sprintf("%d", halfQuota)).
			Obj()
		cq = utiltestingapi.MakeClusterQueue(detCQName).
			Cohort("cohort-0").
			Preemption(kueue.ClusterQueuePreemption{
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
				ReclaimWithinCohort: kueue.PreemptionPolicyAny,
			}).
			ResourceGroup(*fq0).
			Obj()
		lq = utiltestingapi.MakeLocalQueue(detLQName, detNamespace).
			ClusterQueue(detCQName).
			Obj()

		cq1Name := "cq-1-0-cohort-0"
		lq1Name := "lq-1-0-cohort-0"
		cq1 := utiltestingapi.MakeClusterQueue(cq1Name).
			Cohort("cohort-0").
			Preemption(kueue.ClusterQueuePreemption{
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
				ReclaimWithinCohort: kueue.PreemptionPolicyAny,
			}).
			ResourceGroup(*fq1).
			Obj()
		lq1 := utiltestingapi.MakeLocalQueue(lq1Name, detNamespace).
			ClusterQueue(cq1Name).
			Obj()
		extraCQs = append(extraCQs, *cq1)
		extraLQs = append(extraLQs, *lq1)

		head1Scope := p.PreemptorScope
		if head1Scope == "all_cluster" {
			head1Scope = "half_cluster"
			incomingWl, _ = makeIncomingWl("preemptor-0", detLQName, "half_cluster")
		}
		wl1, _ := makeIncomingWl("preemptor-1", kueue.LocalQueueName(lq1Name), head1Scope)
		extraIncomingWls = append(extraIncomingWls, wl1)
		wantVictims = -1 // multi-head overlap victim count is verified by Classic == DeepWAS
	} else {
		fq := utiltestingapi.MakeFlavorQuotas(detFlavorName).
			Resource(corev1.ResourceCPU, fmt.Sprintf("%d", cqNominalQuotaCPU)).
			Obj()
		cq = utiltestingapi.MakeClusterQueue(detCQName).
			Preemption(kueue.ClusterQueuePreemption{
				WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
				ReclaimWithinCohort: kueue.PreemptionPolicyNever,
			}).
			ResourceGroup(*fq).
			Obj()
		lq = utiltestingapi.MakeLocalQueue(detLQName, detNamespace).
			ClusterQueue(detCQName).
			Obj()
	}

	return deterministicPreemptionFixture{
		topology:         topology,
		flavor:           flavor,
		cq:               cq,
		lq:               lq,
		extraCQs:         extraCQs,
		extraLQs:         extraLQs,
		nodes:            nodes,
		admittedWls:      admittedWls,
		incomingWl:       incomingWl,
		extraIncomingWls: extraIncomingWls,
	}, wantVictims
}

type gridResultRow struct {
	caseName        string
	nodes           int
	racks           int
	candidates      int
	podsPerWl       int
	tasMode         string
	quota           string
	spread          bool
	scope           string
	overlapHeads    bool
	classicSnapMs   float64
	classicNomMs    float64
	classicDecMs    float64
	classicAllocMB  float64
	classicGC       uint32
	wasBaseSnapMs   float64
	wasBaseNomMs    float64
	wasBaseDecMs    float64
	wasBaseAllocMB  float64
	wasBaseGC       uint32
	wasCachedSnapMs float64
	wasCachedNomMs  float64
	wasCachedDecMs  float64
	wasCachedAllocMB float64
	wasCachedGC     uint32
	slowdownBase    float64
	slowdownCached  float64
	victims         int
}

func medianCycleMetrics(runs []deterministicCycleMetrics) deterministicCycleMetrics {
	if len(runs) == 1 {
		return runs[0]
	}
	sorted := make([]deterministicCycleMetrics, len(runs))
	copy(sorted, runs)
	slices.SortFunc(sorted, func(a, b deterministicCycleMetrics) int {
		if a.decisionDuration < b.decisionDuration {
			return -1
		}
		if a.decisionDuration > b.decisionDuration {
			return 1
		}
		return 0
	})
	return sorted[len(sorted)/2]
}

func parseEnvIntList(key string, def []int) []int {
	env := os.Getenv(key)
	if env == "" {
		return def
	}
	var out []int
	for _, s := range strings.Split(env, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func parseEnvStrList(key string, def []string) []string {
	env := os.Getenv(key)
	if env == "" {
		return def
	}
	var out []string
	for _, s := range strings.Split(env, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func parseEnvBoolList(key string, def []bool) []bool {
	env := os.Getenv(key)
	if env == "" {
		return def
	}
	var out []bool
	for _, s := range strings.Split(env, ",") {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "true" || s == "1" {
			out = append(out, true)
		} else if s == "false" || s == "0" {
			out = append(out, false)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func appendFileLine(path, content string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = f.WriteString(content)
}

func TestDeterministicTASPreemptionGrid(t *testing.T) {
	nodesList := parseEnvIntList("GRID_NODES", []int{24, 2048})
	candsList := parseEnvIntList("GRID_CANDIDATES", []int{800, 2000})
	podsPerWlList := parseEnvIntList("GRID_PODS_PER_WL", []int{2})
	tasModesList := parseEnvStrList("GRID_TAS_MODES", []string{"required"})
	quotasList := parseEnvStrList("GRID_QUOTAS", []string{"exact", "spare"})
	spreadList := parseEnvBoolList("GRID_SPREAD", []bool{true})
	preemptorList := parseEnvStrList("GRID_PREEMPTOR", []string{"all_cluster"})
	overlapList := parseEnvBoolList("GRID_OVERLAP_HEADS", []bool{false})

	targetUtilPct := 80
	if env := os.Getenv("GRID_TARGET_UTIL"); env != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(env)); err == nil && n > 0 && n <= 100 {
			targetUtilPct = n
		}
	}

	repeats := 1
	if env := os.Getenv("GRID_REPEATS"); env != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(env)); err == nil && n > 0 {
			repeats = n
		}
	}

	modesMap := map[string]bool{"classic": true, "was_cold": true, "was_cached": true}
	if env := os.Getenv("GRID_MODES"); env != "" {
		modesMap = map[string]bool{}
		for _, m := range parseEnvStrList("GRID_MODES", nil) {
			modesMap[m] = true
		}
	}
	runClassic := modesMap["classic"]
	runWASCold := modesMap["was_cold"]
	runWASCached := modesMap["was_cached"]
	runAnyWAS := runWASCold || runWASCached

	pprofDir := os.Getenv("GRID_PPROF_DIR")
	outMD := os.Getenv("GRID_OUTPUT_MD")
	outCSV := os.Getenv("GRID_OUTPUT_CSV")

	mdHeader := "### Deterministic TAS Preemption Grid Search Results\n\n" +
		"| Scenario | Mode | Snapshot | Nomination | Decision | Alloc (Snap+Nom) | GC | Slowdown | Victims |\n" +
		"| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
	if outMD != "" {
		if err := os.MkdirAll(filepath.Dir(outMD), 0o755); err == nil {
			_ = os.WriteFile(outMD, []byte(mdHeader), 0o644)
		}
	}

	var csvWriter *csv.Writer
	var csvFile *os.File
	if outCSV != "" {
		if err := os.MkdirAll(filepath.Dir(outCSV), 0o755); err == nil {
			if f, err := os.Create(outCSV); err == nil {
				csvFile = f
				csvWriter = csv.NewWriter(f)
				_ = csvWriter.Write([]string{
					"case", "nodes", "racks", "candidates", "pods_per_wl", "tas_mode", "quota", "layout", "scope", "overlap_heads",
					"classic_snap_ms", "classic_nom_ms", "classic_dec_ms", "classic_alloc_mb", "classic_gc",
					"was_cold_snap_ms", "was_cold_nom_ms", "was_cold_dec_ms", "was_cold_alloc_mb", "was_cold_gc",
					"was_cached_snap_ms", "was_cached_nom_ms", "was_cached_dec_ms", "was_cached_alloc_mb", "was_cached_gc",
					"slowdown_cold", "slowdown_cached", "victims",
				})
				csvWriter.Flush()
			}
		}
	}
	if csvFile != nil {
		defer func() {
			csvWriter.Flush()
			_ = csvFile.Close()
		}()
	}

	var results []gridResultRow

	for _, nodes := range nodesList {
		numRacks := 4
		nodesPerRack := nodes / numRacks
		if nodesPerRack == 0 {
			numRacks = 1
			nodesPerRack = nodes
		}

		for _, cands := range candsList {
			for _, podsPerWl := range podsPerWlList {
				for _, tasMode := range tasModesList {
					for _, quota := range quotasList {
						for _, spread := range spreadList {
							for _, scope := range preemptorList {
								for _, overlap := range overlapList {
									params := GenericFixtureParams{
										NumRacks:         numRacks,
										NodesPerRack:     nodesPerRack,
										NumCandidates:    cands,
										PodsPerWorkload:  podsPerWl,
										TargetUtilPct:    targetUtilPct,
										TASMode:          tasMode,
										QuotaRegime:      quota,
										PreemptorScope:   scope,
										SpreadCandidates: spread,
										OverlapHeads:     overlap,
									}
									layoutName := "Spread"
									if !spread {
										layoutName = "Packed"
									}
									overlapTag := ""
									if overlap {
										overlapTag = "_Overlap"
									}
									caseName := fmt.Sprintf("%dNodes_%dCands_%dPods_%s_%sQuota_%s_%s%s",
										nodes, cands, podsPerWl, tasMode, quota, layoutName, scope, overlapTag)

									t.Run(caseName, func(t *testing.T) {
										fix, wantVictims := buildGenericDeterministicFixture(params)

										var classic, deepWAS deterministicCycleMetrics
										if runClassic {
											runs := make([]deterministicCycleMetrics, 0, repeats)
											for r := range repeats {
												opts := cycleRunOpts{skipColdSnapshot: true}
												if pprofDir != "" && r == repeats-1 {
													opts.cpuProfPath = filepath.Join(pprofDir, caseName+"_classic_nom.cpu.prof")
													opts.memProfPath = filepath.Join(pprofDir, caseName+"_classic_nom.mem.prof")
												}
												runs = append(runs, runSinglePreemptionCycleWithOpts(t, fix, false, opts))
											}
											classic = medianCycleMetrics(runs)
										}
										if runAnyWAS {
											runs := make([]deterministicCycleMetrics, 0, repeats)
											for r := range repeats {
												opts := cycleRunOpts{skipColdSnapshot: !runWASCold}
												if pprofDir != "" && r == repeats-1 {
													opts.cpuProfPath = filepath.Join(pprofDir, caseName+"_deepwas_nom.cpu.prof")
													opts.memProfPath = filepath.Join(pprofDir, caseName+"_deepwas_nom.mem.prof")
												}
												runs = append(runs, runSinglePreemptionCycleWithOpts(t, fix, true, opts))
											}
											deepWAS = medianCycleMetrics(runs)
										}

										if runClassic && runAnyWAS {
											if wantVictims >= 0 && params.PreemptorScope != "single_rack" {
												if classic.preemptedCount != wantVictims {
													t.Errorf("Classic preempted = %d, want %d", classic.preemptedCount, wantVictims)
												}
												if deepWAS.preemptedCount != wantVictims {
													t.Errorf("DeepWAS preempted = %d, want %d", deepWAS.preemptedCount, wantVictims)
												}
											} else if classic.preemptedCount != deepWAS.preemptedCount {
												t.Logf("Note: victim count differs (Classic=%d, DeepWAS=%d)", classic.preemptedCount, deepWAS.preemptedCount)
											}
										}

										ms := func(d time.Duration) float64 {
											return float64(d.Microseconds()) / 1000.0
										}
										mb := func(b uint64) float64 {
											return float64(b) / (1024.0 * 1024.0)
										}

										cSnap, cNom, cDec := ms(classic.snapshotDuration), ms(classic.nominationDuration), ms(classic.decisionDuration)
										cAlloc := mb(classic.snapMem.allocBytes + classic.nomMem.allocBytes)
										cGC := classic.snapMem.numGC + classic.nomMem.numGC

										wColdSnap := ms(deepWAS.coldSnapshotDuration)
										wNom := ms(deepWAS.nominationDuration)
										wColdDec := ms(deepWAS.coldSnapshotDuration + deepWAS.nominationDuration)
										wColdAlloc := mb(deepWAS.coldSnapMem.allocBytes + deepWAS.nomMem.allocBytes)
										wColdGC := deepWAS.coldSnapMem.numGC + deepWAS.nomMem.numGC

										wCachedSnap := ms(deepWAS.snapshotDuration)
										wCachedDec := ms(deepWAS.decisionDuration)
										wCachedAlloc := mb(deepWAS.snapMem.allocBytes + deepWAS.nomMem.allocBytes)
										wCachedGC := deepWAS.snapMem.numGC + deepWAS.nomMem.numGC

										var slowdownCold, slowdownCached float64
										if cDec > 0 {
											slowdownCold = wColdDec / cDec
											slowdownCached = wCachedDec / cDec
										}

										victims := deepWAS.preemptedCount
										if runClassic {
											victims = classic.preemptedCount
										}

										row := gridResultRow{
											caseName:         caseName,
											nodes:            nodes,
											racks:            numRacks,
											candidates:       cands,
											podsPerWl:        podsPerWl,
											tasMode:          tasMode,
											quota:            quota,
											spread:           spread,
											scope:            scope,
											overlapHeads:     overlap,
											classicSnapMs:    cSnap,
											classicNomMs:     cNom,
											classicDecMs:     cDec,
											classicAllocMB:   cAlloc,
											classicGC:        cGC,
											wasBaseSnapMs:    wColdSnap,
											wasBaseNomMs:     wNom,
											wasBaseDecMs:     wColdDec,
											wasBaseAllocMB:   wColdAlloc,
											wasBaseGC:        wColdGC,
											wasCachedSnapMs:  wCachedSnap,
											wasCachedNomMs:   wNom,
											wasCachedDecMs:   wCachedDec,
											wasCachedAllocMB: wCachedAlloc,
											wasCachedGC:      wCachedGC,
											slowdownBase:     slowdownCold,
											slowdownCached:   slowdownCached,
											victims:          victims,
										}
										results = append(results, row)

										scenarioStr := fmt.Sprintf("%d nodes, %d cands (%dp, %s), %s quota, %s, %s%s",
											nodes, cands, podsPerWl, tasMode, quota, layoutName, scope, overlapTag)

										var rowMD strings.Builder
										firstRow := true
										if runClassic {
											rowMD.WriteString(fmt.Sprintf("| **%s** | **Classic** | %.2f ms | %.2f ms | %.2f ms | %.1f MB | %d | 1.0x | %d |\n",
												scenarioStr, cSnap, cNom, cDec, cAlloc, cGC, classic.preemptedCount))
											firstRow = false
										}
										if runWASCold {
											label := ""
											if firstRow {
												label = "**" + scenarioStr + "**"
												firstRow = false
											}
											rowMD.WriteString(fmt.Sprintf("| %s | **DeepWAS (Cold)** | %.2f ms | %.2f ms | %.2f ms | %.1f MB | %d | **%.1fx** | %d |\n",
												label, wColdSnap, wNom, wColdDec, wColdAlloc, wColdGC, slowdownCold, deepWAS.preemptedCount))
										}
										if runWASCached {
											label := ""
											if firstRow {
												label = "**" + scenarioStr + "**"
											}
											rowMD.WriteString(fmt.Sprintf("| %s | **DeepWAS (Cached)** | %.2f ms | %.2f ms | %.2f ms | %.1f MB | %d | **%.1fx** | %d |\n",
												label, wCachedSnap, wNom, wCachedDec, wCachedAlloc, wCachedGC, slowdownCached, deepWAS.preemptedCount))
										}

										if outMD != "" {
											appendFileLine(outMD, rowMD.String())
										}
										if csvWriter != nil {
											_ = csvWriter.Write([]string{
												caseName, strconv.Itoa(nodes), strconv.Itoa(numRacks), strconv.Itoa(cands),
												strconv.Itoa(podsPerWl), tasMode, quota, layoutName, scope, strconv.FormatBool(overlap),
												fmt.Sprintf("%.2f", cSnap), fmt.Sprintf("%.2f", cNom), fmt.Sprintf("%.2f", cDec), fmt.Sprintf("%.2f", cAlloc), strconv.FormatUint(uint64(cGC), 10),
												fmt.Sprintf("%.2f", wColdSnap), fmt.Sprintf("%.2f", wNom), fmt.Sprintf("%.2f", wColdDec), fmt.Sprintf("%.2f", wColdAlloc), strconv.FormatUint(uint64(wColdGC), 10),
												fmt.Sprintf("%.2f", wCachedSnap), fmt.Sprintf("%.2f", wNom), fmt.Sprintf("%.2f", wCachedDec), fmt.Sprintf("%.2f", wCachedAlloc), strconv.FormatUint(uint64(wCachedGC), 10),
												fmt.Sprintf("%.2f", slowdownCold), fmt.Sprintf("%.2f", slowdownCached), strconv.Itoa(victims),
											})
											csvWriter.Flush()
										}

										t.Logf("[%s] (victims=%d, repeats=%d)\n"+
											"  Classic          : Snap=%8.2fms | Nom=%8.2fms | Dec=%8.2fms | Alloc=%6.1fMB | GC=%d\n"+
											"  DeepWAS (Cold)   : Snap=%8.2fms | Nom=%8.2fms | Dec=%8.2fms | Alloc=%6.1fMB | GC=%d (Slowdown: %5.1fx)\n"+
											"  DeepWAS (Cached) : Snap=%8.2fms | Nom=%8.2fms | Dec=%8.2fms | Alloc=%6.1fMB | GC=%d (Slowdown: %5.1fx)",
											caseName, victims, repeats,
											cSnap, cNom, cDec, cAlloc, cGC,
											wColdSnap, wNom, wColdDec, wColdAlloc, wColdGC, slowdownCold,
											wCachedSnap, wNom, wCachedDec, wCachedAlloc, wCachedGC, slowdownCached)
									})
								}
							}
						}
					}
				}
			}
		}
	}

	if outMD != "" {
		if data, err := os.ReadFile(outMD); err == nil {
			t.Log("\n" + string(data))
		}
	}
}

