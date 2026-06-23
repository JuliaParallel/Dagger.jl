# DataDeps scheduler benchmark suite

Scheduler-comparison harness for `Dagger.spawn_datadeps`. Runs tiled Cholesky
and tiled matrix-multiply under each registered scheduler across a tile-count
sweep, recording per-run wall-clock, AOT-scheduling time, log-derived
execution span, and per-category event totals.

The suite is scheduler-agnostic: adding a new scheduler is one line in
`default_scheduler_factories()` inside `driver.jl`. As metaheuristic and MILP
schedulers come online they slot in beside `RoundRobinScheduler` and
`GreedyScheduler` without further changes.

## Run

From the `Dagger.jl/` repo root:

```
julia --project=. bench/datadeps_schedulers/driver.jl
```

Defaults: both workloads, tile counts `2,4,8`, block size `128`, `1` warmup
and `3` measured trials per cell, output written to
`datadeps_schedulers_results.csv`.

### Recommended invocations

Multi-worker, metrics-warmed, correctness-verified, with summary table — the
configuration intended for paper figures:

```
julia --project=. -p 3 bench/datadeps_schedulers/driver.jl \
    --metrics-warm --check-correctness \
    --output results.csv --summary results.md
```

### Options

| Flag                  | Default                            | Meaning                                                                 |
|-----------------------|------------------------------------|-------------------------------------------------------------------------|
| `--workloads`         | `cholesky,matmul`                  | Comma list                                                              |
| `--tile-counts`       | `2,4,8`                            | `nt` values (matrix side = `nt * block-size`)                           |
| `--block-size`        | `128`                              | Tile side in elements                                                   |
| `--trials`            | `3`                                | Measured trials per cell                                                |
| `--warmup`            | `1`                                | Warmup runs per cell (discarded)                                        |
| `--output`            | `datadeps_schedulers_results.csv`  | CSV path                                                                |
| `--metrics-warm`      | _(off)_                            | Pre-warm global `MetricsTracker` cache so Greedy has real cost data     |
| `--check-correctness` | _(off)_                            | Verify each cell against a dense reference (`L*L'≈A`; `tile_C ≈ A*B`)   |
| `--summary PATH`      | _(off)_                            | Also write a Markdown median-aggregated summary table                   |

## What gets measured

| Column                | Source                                                                          |
|-----------------------|---------------------------------------------------------------------------------|
| `total_wallclock_ms`  | `time_ns()` around the `spawn_datadeps` call                                    |
| `sched_phase_ms`      | `time_ns()` around `datadeps_schedule_dag_aot!` via the `TimedScheduler` wrapper |
| `exec_span_ms`        | Earliest `:datadeps_execute`/`:datadeps_copy` start → latest finish, parsed from logs |
| `n_tasks`             | Count of `:datadeps_execute` finish events                                      |
| `n_copies`            | Count of `:datadeps_copy` + `:datadeps_copy_skip` events                        |
| `copy_total_ms`       | Sum of copy spans                                                               |
| `compute_total_ms`    | Sum of `:compute` spans                                                         |
| `move_total_ms`       | Sum of `:move` spans                                                            |
| `metrics_warm`        | Whether the row was measured under a pre-warmed metrics cache                   |

The wall-clock minus the execution span is a reasonable proxy for AOT
scheduling + orchestration overhead. The explicit `sched_phase_ms` separates
out the AOT pass itself; for schedulers without an AOT method (RoundRobin),
`sched_phase_ms ≈ 0` and the per-task assignment cost shows up inside the
execution span instead.

## Correctness verification

When `--check-correctness` is passed, every cell runs one extra (untimed) pass
that compares the workload's tile output against a dense reference:

- **Cholesky:** reassembles the lower-triangular tiles into `L`, zeros the
  strict upper triangle, and checks `‖L * L' − A‖ / ‖A‖ < 1e-8` against the
  original SPD `A`.
- **Matmul:** reassembles the output tiles into `C` and checks
  `‖C − A * B‖ / ‖A * B‖ < 1e-10` against the dense reference.

A `_correctness.csv` is emitted alongside the timing CSV. Any failure is
logged as a warning, and the run continues so a single bad cell does not
swallow the rest of the sweep.

## Metrics-warm mode

`GreedyScheduler` consults `MT.snapshot(MT.global_metrics_cache())` for
per-signature runtime medians and per-(src, dst) move rates. On a cold cache
both fall back to defaults (`GREEDY_DEFAULT_RUNTIME_NS`,
`GREEDY_DEFAULT_TRANSFER_RATE`), which is *not* the cost model the heuristic
is meant to run under.

`--metrics-warm` runs a single unmeasured `RoundRobinScheduler` pass before
each `(workload, nt, scheduler)` cell. Because every task execution merges
its metrics into the process-global `MT.GLOBAL_METRICS_CACHE`, this pass
populates real runtime and move-rate samples; the measured trials then
exercise Greedy's actual cost model rather than its fallback constants.

## File layout

| File              | Purpose                                                |
|-------------------|--------------------------------------------------------|
| `workloads.jl`    | `tiled_cholesky!`, `tiled_matmul!` and tile builders   |
| `log_analysis.jl` | Log → per-phase totals                                 |
| `driver.jl`       | `TimedScheduler` wrapper, sweep, CSV emit, CLI         |
| `summarize.jl`    | CSV → Markdown median-aggregated table                 |

## Caveats

- The schedule cache is task-local and per-scheduler-type; the driver clears
  it before each measured run so cached schedules from a prior cell do not
  bias the next.
- Log collection adds non-trivial overhead. Warmup runs are deliberately run
  with the same logging state as the measured trials so JIT compilation is
  amortized on the same dispatch specializations, and the per-trial logging
  overhead cancels in cross-scheduler comparisons.
- Single-worker runs assign every task to the same processor regardless of
  scheduler — use `-p N` (with `N ≥ 2`) for cross-scheduler comparisons that
  actually exercise different assignments.
