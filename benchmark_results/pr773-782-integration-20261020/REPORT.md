# PR 773 / 776 / 777 / 782 integration

Integrated into `jps/dev`, starting from `eff8dd5cc4ea99c94d12f859bd036401933e16d3`.
Production source: `05db6197`. No push or publication.

## Changes

- [PR 773](https://github.com/JuliaParallel/Dagger.jl/pull/773): expose independent producers with a FIFO ready frontier, preserving conservative mutable-result alias edges and explicit synchronization dependencies. Includes the MPI Cholesky fixture fix and launch-order benchmark.
- [PR 776](https://github.com/JuliaParallel/Dagger.jl/pull/776): DAGSpec prefetch and typed argument barriers, including the no-extra-workers test fix.
- [PR 777](https://github.com/JuliaParallel/Dagger.jl/pull/777): allocate eligible uninitialized slots and return aliasing in one RPC. Cache reuse remains the first choice.
- [PR 782](https://github.com/JuliaParallel/Dagger.jl/pull/782): ported the arena allocator, replacing the original region-scoped lifetime with explicit **DataDepsContext epoch** ownership. This PR's original `spawn_datadeps` finally/free envelope was not imported.

Arena reservation has a 256 MiB budget per epoch across workers and partition-local pools; excess allocations use ordinary DRefs. Full drains wait for every inflight task, failed sibling, write-back and requested GPU fence before freeing arenas. Targeted drains and drains leaving dirty write-back pending keep the epoch alive. `free=false` still retires arenas once their work is complete. A planning condition prevents foreign drains from reclaiming memory before task and cache-lease retirement publication. Both slot-cache admission paths reject arena handles; cold hierarchical cacheable slots remain DRef-backed. Logging allocates ordinary slots. Identity aliasing of a historical arena descriptor does not dereference expired storage; materializing an expired handle raises an error.

## Correctness and allocations

Julia 1.11.9 was used explicitly with the preserved Project/Manifest/LocalPreferences files in this directory. Distributed drivers have four default threads and workers two default threads. MPI uses two ranks with two default threads plus one interactive thread per rank. Allocation bounds were unchanged. The CUDA sanity check used an RTX 5060 with NVIDIA driver 615.78.08; it did not reuse the old report's driver claim.

| Check | Result |
|---|---|
| CPU, no extra workers | Datadeps 843 pass; scheduling 546 pass / 2 expected broken; hierarchical async 65 pass / 2 expected broken; targeted synchronization 40 pass |
| CPU, three workers | Datadeps 2,277 pass / 1 expected broken; scheduling 529 pass / 1 expected broken; locality 5 pass; hierarchical async 10 pass / 1 expected broken; targeted synchronization 45 pass |
| Final arena regression suite, three workers | 83 pass |
| Allocation bounds, zero and two workers | 24 pass in each topology |
| Arena helper inference / lowered-code checks | 16 pass; no checked helper inferred `Any` or captured `Core.Box` |
| MPI CPU | 663 and 666 pass on the two ranks |
| MPI flat async | 5 and 8 pass |
| MPI hierarchical async | 7 and 12 pass |
| CUDA Distributed | 300 pass / 4 expected broken |

The arena suite enables poison-before-free on all workers and checks async carry-in, targeted/full/foreign/task-exit/state-pressure drains, failed tasks with live siblings, cold and reused hierarchical cache slots, AOT prefill reuse, pristine prefill cleanup, byte-budget fallback and contention, planning/drain races, partition pipeline retirement, and tiled GEMM. Registries are checked empty after full drains. With no workers, arena and locality tests correctly skip because there are no cross-space replicas.

The broad three-worker run preceded the final descriptor-only aliasing and empty-epoch cleanup refinements; the fresh 83-check arena suite, extended inference, allocation, MPI and CUDA checks cover those refinements. Earlier failed attempts and their harness sources remain preserved: channel-valued test arguments were not movable, child test tasks needed explicit testset inheritance, and a CUDA wrapper referenced an unimported module. These were harness errors, not ignored failing assertions in the final runs. The allocation-only runner also logged the eager scheduler's shutdown `InterruptException` after its passing summary, with process exit zero. A baseline workload logged worker removal after saving its successful measurements. The MPI CPU diagnostic profile showed compilation in MINRES; the run completed successfully within its original deadline.

## Paired performance

Baseline is the pre-integration source `eff8dd5c`, preserved in a detached worktree and source archive. Both revisions use the same scripts and package environments. Ten warmups precede GC and seven measured samples. Counts and bytes below are minimum **driver-process GC** deltas; times are sample medians. Every warmup and sample validates the numerical result. These counts exclude native arena allocations and remote-worker memory.

The slot workload uses 16 matrices of 64×64 Float64 values originating on one worker, tasks on a second worker, and eight regions. Both workers have two threads.

| Eight-region mode | Allocations before → after | GC bytes before → after | Median ms before → after |
|---|---:|---:|---:|
| Flat synchronous | 766,087 → 421,950 | 39,004,424 → 22,266,968 | 170.619 → 91.820 |
| Flat asynchronous | 205,161 → 166,791 | 10,445,592 → 8,693,008 | 43.938 → 36.770 |
| Hierarchical synchronous | 458,182 → 460,118 | 24,176,808 → 24,413,168 | 93.237 → 97.852 |
| Hierarchical asynchronous | 189,593 → 188,384 | 9,741,040 → 9,743,480 | 33.994 → 32.798 |

Flat synchronous driver counts fall 44.9%, GC bytes 42.9%, and median time 46.2%. Flat asynchronous counts fall 18.7% and median time 16.3%. Hierarchical cache reuse is preserved: GC bytes change by at most 1%; synchronous median increases 5.0% and asynchronous median decreases 3.5% in this small workload. These are combined integration measurements, not an isolated attribution to any one PR.

A separate small 16×16 working-set check alternates between the driver and worker 3 (three workers, two threads each), with the same warmup/GC/sample protocol.

| Mode | Allocations before → after | GC bytes before → after | Median ms before → after |
|---|---:|---:|---:|
| async/8 regions/hier=false | 18,774 → 16,801 | 1,021,984 → 935,664 | 6.318 → 5.714 |
| async/8 regions/hier=true | 22,871 → 20,566 | 1,295,856 → 1,157,120 | 7.001 → 6.167 |
| empty/hier=false | 92 → 93 | 4,720 → 4,896 | 0.027 → 0.028 |
| empty/hier=true | 92 → 93 | 4,720 → 4,896 | 0.030 → 0.028 |
| sync/1 regions/hier=false | 5,867 → 4,104 | 308,376 → 226,496 | 3.206 → 2.583 |
| sync/1 regions/hier=true | 6,399 → 4,784 | 340,592 → 266,344 | 3.111 → 2.474 |
| sync/8 regions/hier=false | 27,246 → 20,956 | 1,452,440 → 1,158,800 | 8.520 → 6.531 |
| sync/8 regions/hier=true | 32,424 → 25,738 | 1,761,792 → 1,457,536 | 9.239 → 7.417 |

Empty regions add one allocation / 176 bytes. The nonempty working sets use fewer driver allocations in every mode; the baseline one-region flat timings were noisy, so their medians should not be treated as a precise speedup estimate.

The MPI launch-order benchmark used two ranks × two default threads plus one interactive thread, ten warmups and seven samples, with maximum wall time across ranks. Copy counts come from a separate untimed logged call matching both current and legacy move-task names.

| MPI case | Median ms before → after | Planned copies per rank before → after |
|---|---:|---|
| scalar_tree | 202.574 → 102.854 | [0, 0] → [0, 0] |
| fast_tree | 19.084 → 6.949 | [0, 0] → [0, 0] |
| collect | 12.337 → 8.372 | [31, 31] → [31, 31] |
| buffer_region | 5.150 → 5.041 | [0, 0] → [0, 0] |

All numerical checks passed. The scalar tree is 1.97× faster and the fast tree 2.75× faster in this run; collect is 1.47× faster with the same 31 planned copies on each rank. The buffer-region control remains close (5.15 → 5.04 ms). Per-rank allocation counts and full sample lists are preserved in `launch-order-comparison.json`; placement can change each rank's share, so these are not global allocation totals.

## Scope and artifacts

The prior `aot-schedulers-rebased` integration remains in the branch's ancestry. This work does not merge a newer version of that branch. The earlier CPU/CUDA/AMD/Intel × plain/Distributed/MPI validation under `benchmark_results/dev-integration-20261009` was left unchanged; this report does not claim that entire matrix was rerun. AMD and Intel native/backend limitations documented there remain unresolved.

Arena-backed values are borrowed internal replicas. Retaining task-returned replica arrays or raw logged values beyond their owning epoch remains a pre-existing slot-lifetime hazard; this change does not introduce general result ownership or reference counting. Fresh logging uses DRef slots; enabling logging midway through an already live arena epoch does not retroactively convert its replicas.

`metadata.json` records source revisions, exact commands, process ownership, bounded-run status and runtime platform. `sources.json` records fetched PR tips. `baseline-source.tar.gz`, `integrated-source.tar.gz` and `integration.patch` preserve source; `artifact-sha256.json` verifies logs, environments, scripts and results while excluding the redundant detached worktree. Earlier attempts remain alongside final artifacts. The previous validation directory is outside this snapshot and was not modified.
