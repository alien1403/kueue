# Performance Evaluation: TAS Preemption in WAS DeepIntegration vs Classic Kueue
[Răzvan-Mihai Hanghicel](mailto:rhanghicel@google.com) | Nov 10, 2026

#begin-approvals-addon-section
Username | Role | Status | Last change
--- | --- | --- | ---
[olekz](http://teams/olekz) | Reviewer | 🟢 LGTM | Nov 12, 2026
[pkepka](http://teams/pkepka) | Reviewer | 🟡 Pending | Nov 12, 2026

Approval Instructions: Please approve or LGTM through the [G3 Assist](https://goto.google.com/g3a-approvals-reviewing) sidebar.
For more information, see [go/g3a-approvals-reviewing](https://goto.google.com/g3a-approvals-reviewing)

## Context
This doc compares TAS preemption performance between Classic Kueue and WAS DeepIntegration. Earlier runs showed two main problems: NewSimulator recreating virtual pods every cycle, and slow Phase 2 preemption (reprieveVictim) when spare quota is available.

In [PR #18514](https://github.com/kubernetes-sigs/kueue/pull/18514), we cached virtual v1.Pod objects in wasSimulatorFactory (keyed by workload UID and ResourceVersion) so unchanged workloads aren't rebuilt on every cycle. Below are the numbers for Classic TAS, DeepWAS (baseline), and DeepWAS (with PR #18514).

---

## 1. Microbenchmark ([BenchmarkDeterministicTASPreemptionCycle](https://github.com/alien1403/kueue/blob/d1534fe7e1ba15331a1f52a359d18b5fab688053/pkg/scheduler/scheduler_tas_was_preemption_test.go#L805))

**24 nodes (4 racks x 6 nodes), 682 admitted workloads, 1 incoming preemptor (60 victims):**

Mode | Latency (ms/op) | Memory (MB/op) | Allocs / op
--- | --- | --- | ---
Classic TAS | 18.6 ms | 5.9 MB | 72,998
DeepWAS (baseline) | 117.0 ms | 47.6 MB | 225,542
DeepWAS (with PR #18514) | 93.6 ms | 42.7 MB | 194,178

Caching the virtual pods drops cycle latency by ~20% (117 ms -> 93.6 ms) and saves ~31.4k allocations per cycle, though DeepWAS is still ~5x slower than Classic.

**2,048 nodes (4 racks x 512 nodes, 64 CPU/node = 131,072 CPUs), ~58,000 admitted pods, 1 incoming preemptor (60 victims):**

Mode | Latency (ms/op) | Memory (MB/op) | Allocs / op
--- | --- | --- | ---
Classic TAS | 94.0 ms | 21.7 MB | 216,709
DeepWAS (baseline) | 883.6 ms | 315.0 MB | 1,359,668
DeepWAS (with PR #18514) | 907.1 ms | 312.4 MB | 1,346,256

On 2,048 nodes, DeepWAS takes ~900 ms vs ~94 ms for Classic (~9.5x slower) and allocates ~312 MB and 1.35M objects per cycle. Because most of the time is spent in victim reprieval across thousands of nodes, virtual pod caching has minimal impact on the microbenchmark itself (~907 ms vs ~884 ms is single-run variance).

---

## 2. Scaling tests ([TestDeterministicTASPreemptionCycle](https://github.com/alien1403/kueue/blob/d1534fe7e1ba15331a1f52a359d18b5fab688053/pkg/scheduler/scheduler_tas_was_preemption_test.go#L630))

Single-cycle Snapshot and Nomination times across different queue sizes and quota setups (with a warmup Snapshot call so the virtual pod cache is populated):

**24-node topology (4 racks x 6 nodes):**

Scenario | Mode | Snapshot | Nomination | Total Decision
--- | --- | --- | --- | ---
682 workloads, packed<br>(60 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 0.38 ms<br>30.88 ms<br>29.29 ms | 8.43 ms<br>72.68 ms<br>46.22 ms | 8.81 ms<br>103.56 ms<br>75.51 ms (-27%)
800 workloads, exact quota<br>(800 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 0.23 ms<br>41.23 ms<br>20.41 ms (-50%) | 30.29 ms<br>76.71 ms<br>49.62 ms | 30.52 ms<br>117.94 ms<br>70.03 ms (-41%)
800 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 0.27 ms<br>56.32 ms<br>20.54 ms (-63%) | 48.77 ms<br>501.60 ms<br>178.71 ms | 49.04 ms<br>557.92 ms<br>199.24 ms (-64%)
2,000 workloads, exact quota<br>(2,000 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 0.89 ms<br>73.86 ms<br>48.55 ms (-34%) | 80.21 ms<br>159.69 ms<br>152.16 ms | 81.10 ms<br>233.55 ms<br>200.71 ms (-14%)
2,000 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 0.70 ms<br>99.88 ms<br>47.69 ms (-52%) | 141.85 ms<br>798.18 ms<br>424.27 ms | 142.56 ms<br>898.06 ms<br>471.96 ms (-47%)
10,000 workloads, exact quota<br>(10,000 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 7.34 ms<br>577.83 ms<br>272.55 ms (-53%) | 461.99 ms<br>2,074.04 ms<br>1,262.40 ms | 469.33 ms<br>2,651.87 ms<br>1,534.95 ms (-42%)
10,000 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 12.95 ms<br>422.89 ms<br>427.37 ms | 673.07 ms<br>3,331.47 ms<br>3,529.02 ms | 686.01 ms<br>3,754.36 ms<br>3,956.39 ms

Caching cuts Snapshot time by ~34–63% in most cases, and indirectly lowers Nomination in several rows by avoiding Snapshot allocations that otherwise trigger GC during Nomination. At 10,000 workloads, spare quota, the ~3.5s reprieval loop dominates instead (the ~5% jump is single-run noise).

**2,048-node topology (4 racks x 512 nodes, 131,072 CPUs):**

Scenario | Mode | Snapshot | Nomination | Total Decision
--- | --- | --- | --- | ---
800 workloads, exact quota<br>(800 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 3.37 ms<br>102.27 ms<br>28.00 ms (-73%) | 66.86 ms<br>579.05 ms<br>579.05 ms | 70.23 ms<br>681.32 ms<br>607.05 ms (-11%)
800 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 2.42 ms<br>29.45 ms<br>25.28 ms (-14%) | 558.79 ms<br>6,543.00 ms<br>6,535.00 ms | 561.21 ms<br>6.57 s<br>6.56 s
2,000 workloads, exact quota<br>(2,000 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 4.21 ms<br>71.77 ms<br>73.98 ms | 132.17 ms<br>692.73 ms<br>692.72 ms | 136.38 ms<br>764.50 ms<br>766.70 ms
2,000 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 4.34 ms<br>130.93 ms<br>73.38 ms (-44%) | 1.53 s<br>11.81 s<br>11.81 s | 1.53 s<br>11.94 s<br>11.88 s
10,000 workloads, exact quota<br>(10,000 victims) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 11.27 ms<br>455.90 ms<br>304.41 ms (-33%) | 676.02 ms<br>1.15 s<br>1.15 s | 687.29 ms<br>1.61 s<br>1.46 s (-9%)
10,000 workloads, spare quota<br>(reprieveVictim active) | Classic<br>DeepWAS (baseline)<br>DeepWAS (cached) | 13.59 ms<br>402.51 ms<br>462.05 ms | 7.39 s<br>53.36 s<br>53.36 s | 7.40 s<br>53.76 s<br>53.82 s

On 2,048 nodes, caching cuts Snapshot time by up to ~73% (102 ms -> 28 ms at 800 workloads, and 456 ms -> 304 ms at 10k workloads). However, under spare quota with reprieveVictim active, Nomination time becomes the bottleneck on DeepWAS: reaching ~6.5s at 800 workloads, ~11.8s at 2,000 workloads, and ~53.4s at 10,000 workloads (vs ~7.4s for Classic).

---

## 3. End-to-end test (tas-preemptions-1cq)

Streaming 460 workloads (300 small, 100 medium, 60 large) with performance-scheduler-runner on minimalkueue + envtest:
- **User CPU:** 6.1s (Classic) vs 14.0s (DeepWAS) 2.3x higher
- **Peak RSS:** 111 MB (Classic) vs 162 MB (DeepWAS) +46%
- **Evictions:** 39 (Classic) vs 59 (DeepWAS, balanced workloads) +51%

---

## 4. pprof breakdown (tas-preemptions-1cq, DeepWAS baseline)
[pprof-results](https://drive.google.com/drive/folders/1w08JKHb9pNDJj487KOu7kAurbYBgIdq7?resourcekey=0-9yxQmj6uUk3heiDDXQYtcg&usp=drive_link)

CPU time during the baseline run:

What | Time | % of CPU | Notes
--- | --- | --- | ---
Metrics | 2.65s | 17.8% | Scheduler framework collecting and flushing metrics during simulation
GC | 2.17s | 14.5% | Cleaning up ~1 GB of allocations
Scheduling & preemption | 1.46s | 9.8% | Placement and victim checks
Snapshot setup | 1.10s | 7.4% | Rebuilding node and pod state before each cycle

On memory (alloc_space), DeepWAS allocated 992.8 MB in total vs 386.8 MB for Classic (2.5x). 235.6 MB of that went into computing node resource availability (CPU, memory, ports) during NewSnapshot, and 86.6 MB went into virtual pod conversion (fixed by PR #18514).

**Microbenchmark pprof breakdown (2,048 nodes, DeepWAS):**
During the 2,048-node preemption cycle:
- **Upstream Preemption (`EvaluateWorkloadPreemption` & `RunPodGroupPostFilterPlugins`):** ~65% of CPU time and 1.34M allocations (965 MB alloc_space). Iteratively checking victim reprieves repeatedly simulates placement across 2,048 nodes.
- **GC:** ~32% of CPU time. Generating >310 MB of temporary heap allocations per cycle triggers heavy Go GC sweep/mark activity.
- **Snapshot Building:** ~3% of CPU time (effectively reduced by PR #18514).

---

## 5. What is still slow

- **Rebuilding the snapshot in scheduler-library:** Even when Kueue caches the virtual pods, wasSimulatorFactory.NewSimulator (pkg/cache/scheduler/was/simulator_factory.go) still calls cache.NewSnapshot(pods, nodes) every cycle, which re-indexes all nodes and pods from scratch. At 10k workloads that still takes ~270–430 ms vs ~7–13 ms in Classic.
- **reprieveVictim under spare quota:** In Phase 2 preemption, EvaluateWorkloadPreemption tries to put victims back one by one. When the exact quota is tight, PreemptionFilter rejects reprieves immediately. When a spare quota is available, the first ~10–20% of victims pass PreemptionFilter and run a full ScheduleWorkload check, pushing nomination time up to ~180–800 ms on 24 nodes (~3.5s at 10k workloads), and escalating up to **11.8s** (at 2k workloads) and **53.4s** (at 10k workloads) on 2,048 nodes.
- **LeastAllocated scoring:** DeepWAS defaults to LeastAllocated in newWASSchedulerConfig, which spreads small pods across nodes. Classic uses BestFit packing instead. On tas-preemptions-1cq (8 CPU/node), spreading pods left too little contiguous space for 2.5 CPU pods, leading to extra evictions (59 vs 39).

---

## 6. Potential next steps

- **Incremental snapshots in Kueue ([b/569801677](http://b/569801677)):** Use scheduler-library's ClusterState instead of rebuilding cache.NewSnapshot(pods, nodes) every cycle.
- **Investigate reprieveVictim under spare quota:** Check if node filter work during reprieval can be reduced when spare quota lets candidates pass PreemptionFilter.
- **Packing vs. LeastAllocated (to consider):** Check whether bin-packing reduces evictions enough to justify any extra scoring cost.
- **Reuse allocations in preemption:** Pool candidate slices and structs (e.g. sync.Pool) to reduce GC overhead.
