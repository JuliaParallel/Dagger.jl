# MPI (Distributed-Memory Execution)

Dagger can run on top of [MPI](https://juliaparallel.org/MPI.jl/stable/) to
scale across the nodes of a cluster or supercomputer, while keeping the same
programming model you already use with the Distributed backend: ordinary
tasks, [`DArray`s](darray.md), and [Datadeps](datadeps.md) regions. This is implemented as a
package extension (`MPIExt`) that loads automatically when `MPI` is imported
alongside `Dagger`, and is activated by switching Dagger's *acceleration* to MPI:

```julia
using Dagger, MPI
Dagger.accelerate!(:mpi)
```

The remainder of this page explains the execution model, how to launch and
write MPI programs with Dagger, how data ownership and scheduling work across
ranks, how to target specific ranks (and GPUs) with scopes, and how to test
MPI code.

## Distributed vs. MPI: which backend?

Dagger has two execution backends ("accelerations"):

- **Distributed** (the default): a single "head" process drives scheduling and
  farms tasks out to worker processes added with `addprocs`. Workers can be
  added and removed dynamically, and the head process holds the global view of
  the task graph. This is ideal for interactive use, heterogeneous clusters,
  and elastic workloads.
- **MPI**: every rank runs the *same* program (SPMD — Single Program, Multiple
  Data). There is no central head process; instead, all ranks cooperatively and
  deterministically make the same scheduling decisions, and exchange data
  point-to-point over MPI. This is the model expected by most HPC schedulers
  (Slurm, Flux, PBS) and is the right choice when you need to run across many
  nodes launched by `mpiexec`/`srun`/`flux run`.

The key practical difference is *how you launch* and *how data is owned* — the
task and array APIs are shared. Code written against `spawn_datadeps` and
`DArray` will generally run under either backend.

## Requirements and setup

1. Add `MPI` to your project and configure it as usual for MPI.jl (the default
   MPICH JLL works out of the box; use
   [`MPIPreferences`](https://juliaparallel.org/MPI.jl/stable/configuration/)
   to bind a system MPI on a cluster).
2. Import both packages and switch acceleration to MPI:

```julia
using Dagger, MPI
Dagger.accelerate!(:mpi)
```

`accelerate!(:mpi)` initializes MPI if it has not been initialized yet
(`MPI.Init` with thread level `:multiple`), registers one Dagger processor set
per rank, and builds the per-node topology used for local/IPC fast paths. It
uses `MPI.COMM_WORLD` by default.

!!! note "Thread support"
    Dagger uses multiple threads within each rank, so MPI must be initialized
    with `MPI.THREAD_MULTIPLE`. `accelerate!(:mpi)` requests this for you when it
    initializes MPI; if you call `MPI.Init` yourself beforehand, request
    `threadlevel=:multiple`.

## Launching an MPI program

An MPI Dagger program is a normal Julia script launched under `mpiexec` (or your
site's launcher), with one Julia process per rank. Give each rank multiple
threads so Dagger can exploit intra-rank parallelism:

```sh
mpiexec -n 4 julia --project --threads=4 my_program.jl
```

If you do not have a system `mpiexec` handy, Dagger's test helper
`test/run_mpi.jl` launches a script through the `mpiexec` that MPI.jl itself
provides:

```sh
# julia --project=<env> test/run_mpi.jl <nranks> <threads> <script> [args...]
julia --project test/run_mpi.jl 4 4 my_program.jl
```

## The SPMD execution model

Under MPI, **all ranks execute the same code**. When you write:

```julia
Dagger.accelerate!(:mpi)
A = rand(1000, 1000)
DA = DArray(A, Blocks(250, 250))
```

every rank runs these lines. Dagger ensures the *decisions* made are identical
across ranks — which rank owns which chunk, the order in which tasks are
considered, and so on — so that the ranks agree on a single global plan without
any central coordinator. Actual data lives on exactly one rank per chunk (see
[Data ownership](@ref) below), and Dagger inserts the MPI sends/receives needed
to move data between ranks when a task on one rank consumes data owned by
another.

Because correctness depends on every rank agreeing, any *divergence* between
ranks (e.g. seeding an RNG differently per rank, or branching on
`MPI.Comm_rank`) can corrupt the plan. Dagger can catch such divergence early:

```julia
Dagger.check_uniformity!(true)
```

With uniformity checking enabled, Dagger hashes planning-time values and
compares them across ranks, throwing an error at the point of divergence rather
than deadlocking or producing wrong results later. This adds communication
overhead, so it is best used during development and testing; disable it
(`Dagger.check_uniformity!(false)`, the default) for production runs once your
program is known to be uniform.

The same check is available to your own code: call `Dagger.check_uniform(value)`
to assert that `value` is identical across all ranks (it hashes `value` and
compares it across the communicator, throwing if any rank differs). This is
useful for validating that data or parameters your program computes
independently on each rank really are the same everywhere — for example a size,
a seed, or a configuration value you are about to base scheduling decisions on.
Like the automatic checks, `check_uniform` only communicates when uniformity
checking is enabled (`Dagger.check_uniformity!(true)`) and the current
acceleration runs uniformly (e.g. MPI); otherwise it is a no-op that returns
`true`.

!!! warning "Keep planning uniform"
    Do not make Dagger scheduling or array-construction decisions depend on
    `MPI.Comm_rank`, per-rank random state, or other per-rank values. Generate
    data uniformly (e.g. seed RNGs identically on all ranks, or generate on one
    rank and let Dagger distribute it), and let *scopes* — not `if rank == …` —
    express where work should run.

## A first example

A complete MPI program that builds a distributed array, mutates every block
in-place through a datadeps region, and collects the result:

```julia
using Dagger, MPI

Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)   # optional, recommended during development

inc!(X) = (X .+= 1; nothing)

# Every rank runs this identically; Dagger distributes the blocks across ranks.
A = rand(12, 12)
DA = DArray(A, Blocks(4, 4))

Dagger.spawn_datadeps() do
    for chunk in DA.chunks
        Dagger.@spawn inc!(InOut(chunk))
    end
end

# `collect` gathers the (now incremented) blocks; the result is identical on
# every rank.
result = collect(DA)

if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    @show result ≈ (A .+ 1)
end
```

Run it with:

```sh
mpiexec -n 4 julia --project --threads=2 first_example.jl
```

Note that there is no explicit communication in the program — Dagger moves each
block to whichever rank runs its `inc!` task, and `collect` gathers them back.

## Data ownership

Under MPI, a Dagger `Chunk` is owned by exactly one rank. The chunk's
handle is an `MPIRef`, which records the owning rank and (on that rank only) the
local `MemPool` reference to the actual data. On non-owning ranks, the same
chunk is a lightweight, typed placeholder: it carries the chunk's type and size
metadata (so planning stays uniform) but holds no data. When a task needs a
chunk that lives on another rank, Dagger performs the MPI transfer as part of
executing the datadeps region.

This is why array construction is deterministic: `DArray(A, Blocks(...))` assigns
each block to a specific rank using a fixed rule (see [Processor grids](@ref)),
and every rank computes the same assignment.

## Processors and memory spaces

The MPI backend mirrors Dagger's normal [Processors](processors.md) hierarchy, stamped
with rank information:

- `MPIClusterProc` — represents the whole communicator (all ranks).
- `MPIOSProc` — represents a single rank (analogous to an `OSProc`/worker).
- `MPIProcessor` — a rank-local compute processor wrapping an inner processor
  (a `ThreadProc` for CPU work, or a GPU device processor). This is what tasks
  actually run on.
- `MPIMemorySpace` — a rank-stamped [memory space](data-management.md)
  (CPU RAM, or device VRAM) used to track where data resides.

You can enumerate the processors of the cluster (useful for building scopes):

```julia
MPIExt = Base.get_extension(Dagger, :MPIExt)
procs = Dagger.get_processors(MPIExt.MPIClusterProc(MPI.COMM_WORLD))
for p in procs
    @show p.rank, p.innerProc   # e.g. (0, ThreadProc(1, 1)), (1, ThreadProc(1, 2)), ...
end
```

## Scopes: targeting ranks

[Scopes](scopes.md) constrain where a task may run. Under MPI, two scope keys select
by rank:

- `Dagger.scope(mpi_rank=r)` — any processor on rank `r`.
- `Dagger.scope(mpi_ranks=[r1, r2, ...])` or `Dagger.scope(mpi_ranks=:)` — any
  processor on the listed ranks (or on all ranks).

For example, to pin a task to rank 1:

```julia
t = Dagger.@spawn scope=Dagger.scope(mpi_rank=1) my_function(args...)
```

or to constrain a whole region:

```julia
Dagger.with_options(scope=Dagger.scope(mpi_ranks=[0, 1])) do
    Dagger.spawn_datadeps() do
        # ... tasks here may only run on ranks 0 and 1 ...
    end
end
```

Pass an optional `mpi_comm` in the scope specifier to use a communicator other
than `COMM_WORLD`.

## Processor grids

When Dagger constructs a distributed array, it must decide which rank owns each
block. Under MPI this assignment must be identical on every rank, so the MPI
backend supplies a deterministic default *processor grid* that maps blocks to
ranks in a round-robin (cyclic) fashion across the CPU processors of all ranks.
This happens automatically inside array constructors and inside datadeps
regions, so ordinary `DArray`/`Blocks` code "just works" and produces the same
layout on every rank.

Because block placement is deterministic and computed inside the datadeps
machinery, you generally do not need to think about the grid — but it is why
data is spread across ranks without any explicit placement calls on your part.

## Datadeps under MPI

[Datadeps](datadeps.md) is the recommended way to express in-place, dependency-ordered
computation, and it is the most fully-featured path under MPI. The API is
identical to the Distributed backend — `spawn_datadeps`, with `In`, `Out`,
`InOut`, and `Deps` annotations — but the execution semantics differ:

- Argument chunks that live on another rank are transferred via MPI before the
  consuming task runs.
- Writes (`Out`/`InOut`) update the owning rank's copy; dependent tasks on other
  ranks receive the updated data.
- The region completes only once all tasks on all ranks have finished, keeping
  ranks synchronized at region boundaries.

All of the datadeps features documented in [Datadeps](datadeps.md) — read/write
dependency ordering, aliasing of overlapping views, `Deps` for element-wise
dependency modifiers, and [Stencils](stencils.md) — are supported under MPI.

## GPUs with MPI

Dagger's MPI backend composes with its GPU backends (CUDA, ROCm, oneAPI, Metal,
OpenCL). Each rank exposes its local GPU device(s) as `MPIProcessor`s wrapping
the backend's device processor, and datadeps regions can move data between CPU
and GPU memory on any rank, and between ranks.

Load the GPU backend alongside Dagger and MPI, then switch to MPI acceleration:

```julia
using Dagger, MPI, CUDA
Dagger.accelerate!(:mpi)
```

By default, GPU processors are **not** part of the default scope — tasks run on
CPU processors unless you opt in with a scope that includes GPU processors.
The most explicit way to target a specific rank's GPU is to build an
`ExactScope` for that processor:

```julia
MPIExt = Base.get_extension(Dagger, :MPIExt)
CUDAExt = Base.get_extension(Dagger, :CUDAExt)

mpi_procs() = collect(Dagger.get_processors(MPIExt.MPIClusterProc(MPI.COMM_WORLD)))
gpu_proc(r) = first(p for p in mpi_procs()
                    if p.rank == r && p.innerProc isa CUDAExt.CuArrayDeviceProc)
gpu_scope(r) = Dagger.ExactScope(gpu_proc(r))

nranks = MPI.Comm_size(MPI.COMM_WORLD)
# Outer scope must include every GPU processor a region's tasks may use.
all_gpus = Dagger.UnionScope([gpu_scope(r) for r in 0:nranks-1]...)

A = rand(Float32, 8, 8)
add1!(X) = (X .+= 1; nothing)   # broadcast; scalar indexing is illegal on GPU arrays

Dagger.with_options(scope=all_gpus) do
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=gpu_scope(0) add1!(InOut(A))
        Dagger.@spawn scope=gpu_scope(min(1, nranks-1)) add1!(InOut(A))
    end
end
```

To mix CPU and GPU tasks in one region, make the outer scope a `UnionScope` of
both the CPU and GPU processors the region will use.

### GPU-aware MPI

By default the data plane is **host-staged**: GPU data is copied to host memory,
sent over MPI, and copied back to device memory on the destination rank. This
works with any MPI build and needs no special configuration.

If your MPI is GPU-aware (CUDA-aware / ROCm-aware / etc.), Dagger can send device
buffers directly. This is detected per backend and can be forced on or off with
the `DAGGER_MPI_GPU_DIRECT` environment variable (`"1"`/`"0"`). The relevant
hooks — `Dagger.mpi_device_direct`, `Dagger.mpi_device_sync`,
`Dagger.mpi_library_gpu_aware`, and `Dagger.mpi_remap_space` — are overridable
per backend for advanced setups.

## Collecting and fetching results

`collect(DA)` performs a deterministic, rank-uniform gather: it returns the full
array on **every** rank (each rank ends up with an identical copy). This is
convenient for reductions or for writing results out from rank 0.

`fetch` on a chunk or `DTask` resolves data locally: the owning rank returns its
payload, while non-owning ranks return their placeholder metadata. Guard
rank-specific output (printing, file writes) with `if MPI.Comm_rank(comm) == 0`.

## Testing MPI code

Dagger ships a set of MPI test environments and suites (used by CI) that are a
good template for your own tests:

- CPU MPI environment: `test/mpienv` (deps: `Dagger`, `MPI`, ...).
- GPU MPI environments: `test/cudaenv`, `test/rocmenv`, `test/openclenv`,
  `test/metalenv`, `test/oneapienv` (each adds its backend).
- CPU suite: `test/mpi.jl`. Shared GPU suite: `test/mpi_gpu_suite.jl`, driven by
  per-backend entry points `test/mpi_cuda.jl`, `test/mpi_rocm.jl`,
  `test/mpi_opencl.jl`, `test/mpi_metal.jl`, `test/mpi_oneapi.jl`.

Run a suite directly through MPI.jl's bundled `mpiexec` (no system MPI needed):

```sh
# CPU suite, 4 ranks, 2 threads each
julia --project=test/mpienv test/run_mpi.jl 4 2 test/mpi.jl

# CUDA GPU suite, 2 ranks
julia --project=test/cudaenv test/run_mpi.jl 2 2 test/mpi_cuda.jl
```

## Limitations and tips

- **Keep planning uniform.** The single most important rule: every rank must
  make the same Dagger decisions. Use `Dagger.check_uniformity!(true)` while
  developing to catch divergence.
- **Use scopes, not `if rank == …`, to place work.** Expressing placement with
  scopes keeps the program uniform while still directing tasks to specific
  ranks or devices.
- **Datadeps is the primary supported model** for in-place, dependency-ordered
  computation under MPI; prefer it over ad-hoc `@spawn`/`fetch` chains for
  data-parallel workloads.
- **GPU processors are opt-in** via scopes; nothing runs on a GPU unless a scope
  selects it.
- **`collect` returns the whole array on every rank** — use it deliberately for
  large arrays, and guard rank-specific I/O with a rank check.
