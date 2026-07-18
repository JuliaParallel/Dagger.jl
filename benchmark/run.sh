#!/usr/bin/env bash
# Convenience wrapper for running the Dagger benchmark suite standalone (i.e.
# without AirspeedVelocity), against your local working-tree checkout of
# Dagger -- not whatever version happens to be pinned in
# benchmark/Manifest.toml.
#
# Usage:
#   benchmark/run.sh [julia args...]
#
# All BENCHMARK_* environment variables documented at the top of
# benchmarks.jl are honored as usual, e.g. a quick smoke test:
#
#   BENCHMARK="array:dagger" BENCHMARK_SCALE=10 benchmark/run.sh
#
# Unlike AirspeedVelocity (which defaults to Dagger-only, since a plain native
# array has nothing to do with the Dagger revision being compared), this
# standalone runner defaults BENCHMARK to *also* run the "raw" (non-Dagger,
# native-array) variant wherever one exists (array/linalg; "sparse"/"stencil"
# are Dagger-only), so `benchmark/plot.jl` has a non-Dagger baseline to plot
# alongside "dagger" out of the box. Set BENCHMARK explicitly to override, e.g.
# to go back to Dagger-only:
#
#   BENCHMARK="array:dagger;linalg:dagger;sparse:dagger;stencil:dagger" benchmark/run.sh
#
# How it works
# ------------
# Julia's package loading walks `JULIA_LOAD_PATH` (an "environment stack") in
# order, resolving each `using`/`import` from the first entry that provides
# it. We stack, in order:
#   1. This repo's root -- its Project.toml is Dagger's own, so it
#      self-resolves `using Dagger` straight to this checkout's `src/`.
#   2. `@`, the active project (`--project=benchmark`), which supplies
#      BenchmarkTools/JSON3/DTables/etc.
#   3. `@stdlib`, for Distributed/LinearAlgebra/Random/etc.
#
# The order matters: putting `@` *before* the repo root would instead resolve
# Dagger from whatever version is pinned in benchmark/Manifest.toml (pulled in
# transitively via DTables), silently ignoring any local changes -- the exact
# opposite of what you want when benchmarking a working tree.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export JULIA_LOAD_PATH="${REPO_ROOT}:@:@stdlib"
export BENCHMARK="${BENCHMARK:-array:dagger,raw;linalg:dagger,raw;stencil:dagger}"

exec julia --project="${REPO_ROOT}/benchmark" "${REPO_ROOT}/benchmark/benchmarks.jl" "$@"
