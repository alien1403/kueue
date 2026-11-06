---
name: was-tas-preemption-perf
description: Run and analyze Topology-Aware Scheduling (TAS) preemption performance benchmarks comparing Classic Kueue TAS against DeepWAS (scheduler-library integration). Use when benchmarking TAS/WAS scheduling, preemption latency, candidate scaling, or profiling minimalkueue and single-cycle preemption tests.
license: Apache-2.0
metadata:
  copyright: The Kubernetes Authors
---

# WAS & TAS Preemption Performance Benchmarks

Runs and analyzes the TAS preemption performance suites comparing **Classic Kueue TAS** (`--enableTAS=true`) against **DeepWAS** (`--enableTAS=true --enableDeepWAS=true`, powered by `sigs.k8s.io/scheduler-library`).

The benchmark suite consists of two complementary layers:
1. **End-to-End Multi-Workload Scheduler Runner (`test/performance/scheduler`)**: Spawns `envtest` (`etcd` + `kube-apiserver`) and `minimalkueue`, streams hundreds or thousands of TAS workloads over time, and records wall time, CPU/memory usage, eviction counts, admission latencies, and `pprof` profiles.
2. **Deterministic Single-Cycle Preemption Benchmarks (`pkg/scheduler/scheduler_tas_was_preemption_test.go`)**: Constructs an identical in-memory cluster, topology, ClusterQueue, and pre-admitted candidate pool (`800`, `2,000`, or `10,000` candidates on `24` or `2,048` nodes) and executes a single scheduling cycle (`Snapshot` + `Nomination` = `Decision`) side-by-side in both Classic and DeepWAS modes.

---

## Part 1: How to Run the End-to-End Scheduler Benchmarks

### Step 1: Build the binaries and set `KUBEBUILDER_ASSETS`

```bash
go build -o bin/minimalkueue ./test/performance/scheduler/minimalkueue
go build -o bin/performance-scheduler-runner ./test/performance/scheduler/runner
export KUBEBUILDER_ASSETS="$(./bin/setup-envtest use -p path)"
```

### Step 2: Run Classic TAS vs. DeepWAS on a generator configuration

Always create the output directory before invoking `performance-scheduler-runner`.

```bash
CONFIG=./test/performance/scheduler/configs/tas-preemptions-1cq/generator.yaml
mkdir -p /tmp/out-classic /tmp/out-deepwas

# Classic Kueue TAS baseline
./bin/performance-scheduler-runner \
  --crds=./config/components/crd/bases \
  --minimalKueue=./bin/minimalkueue \
  --generatorConfig="${CONFIG}" \
  --o=/tmp/out-classic \
  --enableTAS=true \
  --withCPUProf=true \
  --withMemProfile=true \
  --withLogs=true \
  --logToFile=true \
  --logLevel=2

# DeepWAS mode (enables SchedulerLibraryIntegration, SchedulerLibraryDeepIntegration,
# GenericWorkload, CompositePodGroup, and TopologyAwareWorkloadScheduling)
./bin/performance-scheduler-runner \
  --crds=./config/components/crd/bases \
  --minimalKueue=./bin/minimalkueue \
  --generatorConfig="${CONFIG}" \
  --o=/tmp/out-deepwas \
  --enableTAS=true \
  --enableDeepWAS=true \
  --withCPUProf=true \
  --withMemProfile=true \
  --withLogs=true \
  --logToFile=true \
  --logLevel=2
```

### Step 3: Windowed CPU profiling for delayed preemption waves

In configurations where initial seconds are spent admitting candidate workloads before preemptors arrive (such as `tas-preemptions-1cq-many-candidates`, where preemptors arrive at `t = 20s`), use `--cpuProfileStartDelay` and `--cpuProfileDuration` so the profile captures the preemption window:

```bash
mkdir -p /tmp/out-deepwas-800cand
./bin/performance-scheduler-runner \
  --crds=./config/components/crd/bases \
  --minimalKueue=./bin/minimalkueue \
  --generatorConfig=./test/performance/scheduler/configs/tas-preemptions-1cq-many-candidates/generator.yaml \
  --o=/tmp/out-deepwas-800cand \
  --enableTAS=true \
  --enableDeepWAS=true \
  --withCPUProf=true \
  --cpuProfileStartDelay=20s \
  --cpuProfileDuration=25s \
  --cpuProfileCount=1 \
  --withLogs=true \
  --logToFile=true \
  --logLevel=2
```

### Step 4: Inspect end-to-end output artifacts

Each run writes the following files into the output directory:
- `summary.yaml`: Total wall time (`wallMs`), average ClusterQueue CPU usage, total evictions (`totalEvictions`), and per-workload-class admission latency and eviction counts.
- `minimalkueue.stats.yaml`: Total `minimalkueue` process CPU time (`userMs`, `sysMs`) and peak resident memory (`maxrss` in KB).
- `wlStates.csv` and `cqStates.csv`: Time-series state transitions for every workload and ClusterQueue.
- `minimalkueue.cpu.prof` and `minimalkueue.mem.prof`: Profiles inspectable via `go tool pprof -top -cum /tmp/out-deepwas/minimalkueue.cpu.prof`.
- `minimalkueue.err.log`: Structured scheduler logs containing per-cycle durations and preemption target counts.

---

## Part 2: End-to-End Scenarios and Why Each Is Relevant

All end-to-end TAS preemption scenarios use DeepWAS-compatible TAS constraints across three priority tiers (`small` priority `50`, `medium` priority `100`, `large` priority `200`) and two topology request types:
- **`required`** (`tasConstraint: "required"` on `cloud.provider.com/topology-rack`), mapped in DeepWAS to a virtual `PodGroup` with a `TopologyConstraint`.
- **`balanced`** (`tasConstraint: "balanced"` with `sliceSize` on `cloud.provider.com/topology-rack`), mapped in DeepWAS to a virtual `CompositePodGroup` with sliced leaf `PodGroup`s.

### 1. Multi-Cohort / 16-ClusterQueue Scenarios (`4 cohorts x 4 CQs`, `48` nodes = `8 racks x 6 nodes`)

| Config Directory | Node Capacity | Workloads | What It Tests & Why It Is Relevant |
|---|---|---|---|
| `tas-preemptions` | Tight (`8 CPU/node` = `384 CPU`) | `2,560` | **Multi-CQ overlap & tight node packing**: Exercises within-CQ priority preemption, cohort reclamation (`reclaimWithinCohort: Any`), and in-cycle multi-CQ assignment overlap (`Scheduler.updateAssignmentIfNeeded` / `SimulatePodRemoval`). Relevant for detecting state-version invalidation and virtual pod booking conflicts when multiple ClusterQueues nominate workloads onto the same tight nodes in a single cycle. |
| `tas-preemptions-2` | Ample (`96 CPU/node` = `4,608 CPU`) | `2,560` | **Pure CQ/cohort quota preemption at 16-CQ scale**: Identical workload streams and quotas as `tas-preemptions`, but with `96 CPU/node` so node topology capacity is never a bottleneck. Relevant for isolating quota-driven preemption (`PreemptionFilter`) from node-capacity fragmentation across 16 CQs. |
| `tas-preemptions-3` | Ample (`96 CPU/node` = `4,608 CPU`) | `1,984` | **Sustained periodic preemption waves**: Paces `medium` (`12 CPU`, `300ms` runtime) and `large` (`20 CPU`, `200ms` runtime) arrivals to consume ~45% of each CQ's `20 CPU` quota over `30s` while `small` (`4 CPU`, `1,200ms` runtime) workloads continuously refill the CQs between waves. Relevant for measuring steady-state preemption throughput without queue starvation. |

### 2. Single-ClusterQueue Scenarios (`1 cohort x 1 CQ`)

| Config Directory | Topology & Capacity | Workloads | What It Tests & Why It Is Relevant |
|---|---|---|---|
| `tas-preemptions-1cq` | `12` nodes (`2 racks x 6 nodes`, `8 CPU/node` = `96 CPU`), `80 CPU` CQ quota | `460` | **Single-CQ node-scoring & fragmentation comparison**: Eliminates cross-CQ overlap within a scheduling cycle while preserving the `8 CPU/node` tight geometry and `83.3%` quota-to-capacity ratio. Relevant because it exposes how DeepWAS's `kube-scheduler` `LeastAllocated` node scoring spreads `2.0 CPU` `small` pods across nodes (`6.0 CPU` used, `2.0 CPU` free), forcing pure TAS node-fragmentation preemptions when `2.5 CPU/pod` `large` workloads arrive, whereas Classic TAS `BestFit` bin-packs nodes (`8.0 CPU` used) and leaves other nodes empty. |
| `tas-preemptions-1cq-heavy` | `12` nodes (`2 racks x 6 nodes`, `8 CPU/node` = `96 CPU`), `80 CPU` CQ quota | `340` | **Continuous single-CQ saturation**: Increases `small` workload runtime to `3,500ms` so the `80 CPU` ClusterQueue remains 100% full between `medium` (`350ms`) and `large` (`250ms`) waves. Relevant for ensuring every arriving high-priority workload must preempt running lower-priority workloads. |
| `tas-preemptions-1cq-many-candidates` | `24` nodes (`4 racks x 6 nodes`, `8 CPU/node` = `192 CPU`), `160 CPU` CQ quota | `1,100` (`800` simultaneous active candidates) | **High-candidate scaling (`800` candidates / `1,600` virtual pods)**: Admits `800` `small` workloads (`2 pods x 100m = 0.2 CPU`, `40,000ms` runtime) during the first `20s`, then sends `60` `medium` (`8 CPU`, needing `>= 40` victims) and `40` `large` (`20 CPU`, needing `>= 100` victims) preemptors. Relevant for measuring $O(W^2)$ ramp-up overhead in `Cache.Snapshot` (`wasSimulatorFactory.NewSimulator`) and multi-hundred-candidate preemption evaluation under realistic API server load. |

---

## Part 3: How to Run the Deterministic Single-Cycle Preemption Benchmarks

In end-to-end runs, once the first few scheduling cycles differ in duration or node placement, Classic and DeepWAS face different sets of running workloads on subsequent cycles. `pkg/scheduler/scheduler_tas_was_preemption_test.go` eliminates all asynchronous variance by constructing an identical in-memory state and timing the two phases of `Scheduler.schedule`:
- **`Snapshot`** (`sched.cache.Snapshot(ctx)`): Clones ClusterQueue/TAS state and (in DeepWAS) builds virtual pods, the `scheduler-library` `ClusterSnapshot`, and `FrameworkMap`.
- **`Nomination`** (`sched.nominate(ctx, heads, snapshot)`): Runs initial placement and preemption target selection (`nativePlanner` vs. `wasPlanner` -> `ScheduleWorkload` -> `EvaluateWorkloadPreemption`), excluding async API server status patch and eviction I/O.
- **`Decision`** (`Snapshot + Nomination`): Total synchronous scheduling decision time for the cycle.

### Commands to run the deterministic single-cycle suites

```bash
# Run all 24-node and 2,048-node deterministic single-cycle subtests
go test -v -timeout 20m -run TestDeterministicTASPreemptionCycle ./pkg/scheduler

# Run only the 24-node subtests (single-rack preemptors + whole-cluster preemptors)
go test -v -run "TestDeterministicTASPreemptionCycle/24Nodes" ./pkg/scheduler

# Run only the 2,048-node large-scale subtests (800, 2,000, and 10,000 victims)
go test -v -timeout 15m -run "TestDeterministicTASPreemptionCycle/2048Nodes" ./pkg/scheduler

# Run a specific candidate scale across all 3 ClusterQueue quota regimes
go test -v -run "TestDeterministicTASPreemptionCycle/2048Nodes_All2000Evicted" ./pkg/scheduler

# Run the Go testing.B benchmark with memory allocation stats and CPU profile
go test -bench=BenchmarkDeterministicTASPreemptionCycle -benchmem \
  -run=^$ -cpuprofile=/tmp/preempt_cycle.cpu.prof ./pkg/scheduler
```

---

## Part 4: Deterministic Single-Cycle Scenarios and Why Each Is Relevant

### 1. Single-Rack Preemptor Scenarios (`24Nodes_Cycle812_*` and `24Nodes_FullCQ_*`)

- **Setup**: `24` nodes (`4 racks x 6 nodes`, `8 CPU/node = 192 CPU`), `160 CPU` CQ quota, pre-populated with `680` (`Cycle812`), `800`, `2,000`, or `10,000` `small` candidates interleaved across the 4 racks. An incoming `large-required-rack` preemptor requests `8 pods x 2.5 CPU = 20 CPU` on a single rack (`wantVictims` = `60`, `100`, `250`, or `1,250`).
- **Why both `NodePackedWithinRack` and `SpreadAcrossNodes` are tested**:
  - `NodePackedWithinRack` reproduces Classic TAS's `BestFit` placement (candidates packed onto the first nodes of each rack, leaving one node per rack empty).
  - `SpreadAcrossNodes` reproduces DeepWAS's `LeastAllocated` placement (candidates spread evenly across all 6 nodes per rack).
  - Testing both layouts on both schedulers separates algorithmic runtime complexity from placement-induced candidate scanning differences.

### 2. Whole-Cluster Preemptor Scenarios (`All800Evicted`, `All2000Evicted`, `All10000Evicted` on `24` and `2,048` Nodes)

- **Setup**:
  - **`24Nodes_All*Evicted`**: `24` nodes (`4 racks x 6 nodes`, `8 CPU/node = 192 CPU`), incoming preemptor requests `48 pods x 4 CPU = 192 CPU` (`100%` of cluster CPU).
  - **`2048Nodes_All*Evicted`**: `2,048` nodes (`4 racks x 512 nodes`, `64 CPU/node = 131,072 CPU`, standard `110` pods/node limit, larger candidate pods of `2x25 CPU`, `2x20 CPU`, or `2x4 CPU`), incoming preemptor requests `2,048 pods x 64 CPU = 131,072 CPU` (`100%` of cluster CPU).
- **Why a whole-cluster preemptor is relevant**: Because the preemptor requires `100%` of every node's capacity, **all $N$ candidates (`800`, `2,000`, or `10,000`) must be evicted** before the preemptor can fit, guaranteeing an identical victim set (`wantVictims = N`) in both Classic and DeepWAS.
- **Why three ClusterQueue quota regimes (`ExactQuota`, `SpareQuota`, `NodeBoundQuota`) are tested at every scale**:
  1. **`ExactQuota` (`nominalQuota` = cluster capacity, `0` free CQ quota)**: Both ClusterQueue quota and node capacity require evicting all $N$ candidates. In DeepWAS, `PreemptionFilter` rejects `ScheduleWorkload` until all $N$ candidates have been removed in `EvaluateWorkloadPreemption`'s Phase 1 loop, and then Phase 2 (`reprieveVictim`) attempts to put each of the $N$ victims back one by one (where `PreemptionFilter` rejects each in $O(1)$ before node placement).
  2. **`SpareQuota` (`nominalQuota` slightly above cluster capacity)**: `PreemptionFilter` is satisfied after ~`80%`–`90%` of candidates are removed in Phase 1; the remaining ~`10%`–`20%` are removed for node capacity, and in Phase 2 (`reprieveVictim`), `PreemptionFilter` passes for the first ~`10%`–`20%` of victims so each triggers a full `ScheduleWorkload` TAS placement attempt.
  3. **`NodeBoundQuota` (`nominalQuota` covers all running candidates plus the incoming preemptor)**: The incoming preemptor fits in ClusterQueue quota immediately (`RepresentativeMode() == Fit` initially), so `PreemptionFilter` is never registered. This isolates pure node-capacity preemption and verifies the `scheduler-library` two-phase `EvaluateWorkloadPreemption` optimization (evicting all eligible candidates at once in `tryPreemptAllEligible`, verifying the workload fits in 1 call, and then running $N$ `reprieveVictim` checks).
