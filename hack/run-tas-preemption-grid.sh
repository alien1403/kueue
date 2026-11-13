#!/usr/bin/env bash

# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Default parameters
NODES="${GRID_NODES:-24,2048}"
CANDIDATES="${GRID_CANDIDATES:-800,2000}"
PODS_PER_WL="${GRID_PODS_PER_WL:-2}"
TAS_MODES="${GRID_TAS_MODES:-required}"
QUOTAS="${GRID_QUOTAS:-exact,spare}"
SPREAD="${GRID_SPREAD:-true}"
PREEMPTORS="${GRID_PREEMPTOR:-all_cluster}"
OVERLAP_HEADS="${GRID_OVERLAP_HEADS:-false}"
TARGET_UTIL="${GRID_TARGET_UTIL:-80}"
MODES="${GRID_MODES:-classic,was_cold,was_cached}"
REPEATS="${GRID_REPEATS:-1}"
TEST_FILTER=""
PPROF_DIR="${GRID_PPROF_DIR:-}"
TIMEOUT="35m"
OUTPUT_MD="${GRID_OUTPUT_MD:-}"
OUTPUT_CSV="${GRID_OUTPUT_CSV:-}"

print_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Runs a parameter grid search across TAS / WAS preemption scenarios,
comparing Classic TAS against DeepWAS (Cold) and DeepWAS (Cached) with
per-phase latency, allocation, GC, and optional pprof capture.

Presets:
  --all         Run full matrix (Nodes: 24,512,2048 x Cands: 800,2000,10000 x Pods/Wl: 2,8 x TAS: required,balanced x Quotas: exact,spare,node_bound x Layout: spread,packed x Scope: single_rack,half_cluster,all_cluster x Overlap: false,true)
  --full        Run standard comprehensive matrix (Nodes: 24,512,2048 x Cands: 800,2000 x Pods/Wl: 2,8 x TAS: required,balanced x Quotas: exact,spare,node_bound x Layout: spread,packed)
  --2k          Run 2,048-node bottleneck exploration (800,2000,10000 cands x 2,8 pods x required,balanced x exact,spare,node_bound x single_rack,all_cluster)
  --quick       Run a fast sanity grid on 24 nodes (800 cands, exact & spare, 3 repeats)

Custom Parameters:
  -n, --nodes NODES          Comma-separated node counts (default: "${NODES}")
  -c, --candidates CANDS     Comma-separated candidate counts (default: "${CANDIDATES}")
      --pods-per-wl PODS     Comma-separated pods per candidate workload (default: "${PODS_PER_WL}")
      --tas-modes MODES      Comma-separated TAS modes: required,balanced (default: "${TAS_MODES}")
  -q, --quotas QUOTAS        Comma-separated quota regimes: exact,spare,spare_20%,node_bound (default: "${QUOTAS}")
  -l, --layout LAYOUT        "spread", "packed", or "spread,packed" (default: "spread")
  -p, --preemptors SCOPES    Comma-separated: single_node,single_rack,half_cluster,all_cluster (default: "${PREEMPTORS}")
      --overlap-heads VALS   Comma-separated: false,true (2-CQ cohort overlap for committedVictims) (default: "${OVERLAP_HEADS}")
      --target-util PCT      Cluster CPU utilization target % for candidate sizing (default: "${TARGET_UTIL}")
  -m, --modes MODES          Comma-separated: classic,was_cold,was_cached (default: "${MODES}")
  -r, --repeats N            Repetitions per cell, reporting median (default: "${REPEATS}")
  -f, --filter REGEX         Subtest regex filter passed to -run TestDeterministicTASPreemptionGrid/<REGEX>
      --pprof-dir DIR        Capture CPU & heap profiles during Nomination into DIR
  -o, --output FILE          Write streaming markdown results table to FILE
      --csv FILE             Write streaming CSV results to FILE
  -t, --timeout TIMEOUT      Go test timeout (default: "${TIMEOUT}")
  -h, --help                 Show this help message

Examples:
  # Run the full 2,048-node bottleneck matrix
  $0 --2k

  # Isolate pods-per-workload vs candidate-count scaling on 2k nodes with 3 repeats
  $0 -n 2048 -c 800,2000 --pods-per-wl 2,8 -q exact,spare -r 3

  # Capture pprof on a specific 2k-node spare-quota outlier
  $0 -n 2048 -c 2000 -q spare --modes was_cached --pprof-dir /tmp/pprof-2k
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            NODES="24,512,2048"
            CANDIDATES="800,2000,10000"
            PODS_PER_WL="2,8"
            TAS_MODES="required,balanced"
            QUOTAS="exact,spare,node_bound"
            SPREAD="true,false"
            PREEMPTORS="single_rack,half_cluster,all_cluster"
            OVERLAP_HEADS="false,true"
            TIMEOUT="180m"
            shift
            ;;
        --full)
            NODES="24,512,2048"
            CANDIDATES="800,2000"
            PODS_PER_WL="2,8"
            TAS_MODES="required,balanced"
            QUOTAS="exact,spare,node_bound"
            SPREAD="true,false"
            PREEMPTORS="all_cluster"
            OVERLAP_HEADS="false"
            TIMEOUT="90m"
            shift
            ;;
        --2k)
            NODES="2048"
            CANDIDATES="800,2000,10000"
            PODS_PER_WL="2,8"
            TAS_MODES="required,balanced"
            QUOTAS="exact,spare,node_bound"
            SPREAD="true"
            PREEMPTORS="single_rack,all_cluster"
            OVERLAP_HEADS="false,true"
            TIMEOUT="60m"
            shift
            ;;
        --quick)
            NODES="24"
            CANDIDATES="800"
            PODS_PER_WL="2"
            TAS_MODES="required"
            QUOTAS="exact,spare"
            SPREAD="true"
            PREEMPTORS="all_cluster"
            OVERLAP_HEADS="false"
            REPEATS="3"
            TIMEOUT="5m"
            shift
            ;;
        -n|--nodes)
            NODES="$2"
            shift 2
            ;;
        -c|--candidates)
            CANDIDATES="$2"
            shift 2
            ;;
        --pods-per-wl)
            PODS_PER_WL="$2"
            shift 2
            ;;
        --tas-modes)
            TAS_MODES="$2"
            shift 2
            ;;
        -q|--quotas)
            QUOTAS="$2"
            shift 2
            ;;
        -l|--layout)
            if [[ "$2" == *"spread"* && "$2" == *"packed"* ]]; then
                SPREAD="true,false"
            elif [[ "$2" == "packed" ]]; then
                SPREAD="false"
            else
                SPREAD="true"
            fi
            shift 2
            ;;
        -p|--preemptors)
            PREEMPTORS="$2"
            shift 2
            ;;
        --overlap-heads)
            OVERLAP_HEADS="$2"
            shift 2
            ;;
        --target-util)
            TARGET_UTIL="$2"
            shift 2
            ;;
        -m|--modes)
            MODES="$2"
            shift 2
            ;;
        -r|--repeats)
            REPEATS="$2"
            shift 2
            ;;
        -f|--filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        --pprof-dir)
            PPROF_DIR="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_MD="$2"
            shift 2
            ;;
        --csv)
            OUTPUT_CSV="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            print_usage >&2
            exit 1
            ;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "${REPO_ROOT}/artifacts"
if [[ -z "${OUTPUT_MD}" ]]; then
    OUTPUT_MD="${REPO_ROOT}/artifacts/preemption_grid_${TIMESTAMP}.md"
fi
if [[ -z "${OUTPUT_CSV}" ]]; then
    OUTPUT_CSV="${OUTPUT_MD%.md}.csv"
fi

echo "=========================================================="
echo " Starting Deterministic TAS Preemption Grid Search"
echo "=========================================================="
echo " Nodes         : ${NODES}"
echo " Candidates    : ${CANDIDATES}"
echo " Pods/Workload : ${PODS_PER_WL}"
echo " TAS Modes     : ${TAS_MODES}"
echo " Quotas        : ${QUOTAS}"
echo " Spread        : ${SPREAD}"
echo " Preemptor     : ${PREEMPTORS}"
echo " Overlap Heads : ${OVERLAP_HEADS}"
echo " Target Util   : ${TARGET_UTIL}%"
echo " Modes         : ${MODES}"
echo " Repeats       : ${REPEATS}"
echo " Timeout       : ${TIMEOUT}"
echo " Markdown Out  : ${OUTPUT_MD}"
echo " CSV Out       : ${OUTPUT_CSV}"
if [[ -n "${PPROF_DIR}" ]]; then
    echo " Pprof Dir     : ${PPROF_DIR}"
fi
echo "=========================================================="

export GRID_NODES="${NODES}"
export GRID_CANDIDATES="${CANDIDATES}"
export GRID_PODS_PER_WL="${PODS_PER_WL}"
export GRID_TAS_MODES="${TAS_MODES}"
export GRID_QUOTAS="${QUOTAS}"
export GRID_SPREAD="${SPREAD}"
export GRID_PREEMPTOR="${PREEMPTORS}"
export GRID_OVERLAP_HEADS="${OVERLAP_HEADS}"
export GRID_TARGET_UTIL="${TARGET_UTIL}"
export GRID_MODES="${MODES}"
export GRID_REPEATS="${REPEATS}"
export GRID_OUTPUT_MD="${OUTPUT_MD}"
export GRID_OUTPUT_CSV="${OUTPUT_CSV}"
if [[ -n "${PPROF_DIR}" ]]; then
    export GRID_PPROF_DIR="${PPROF_DIR}"
fi

RUN_REGEX="TestDeterministicTASPreemptionGrid"
if [[ -n "${TEST_FILTER}" ]]; then
    RUN_REGEX="TestDeterministicTASPreemptionGrid/${TEST_FILTER}"
fi

go test -v -timeout "${TIMEOUT}" -run "${RUN_REGEX}" ./pkg/scheduler

if [[ -f "${OUTPUT_MD}" ]]; then
    echo ""
    echo "=========================================================="
    echo " Grid Search Complete! Summary Table:"
    echo "=========================================================="
    cat "${OUTPUT_MD}"
    echo ""
    echo "Markdown report : ${OUTPUT_MD}"
    echo "CSV report      : ${OUTPUT_CSV}"
fi
