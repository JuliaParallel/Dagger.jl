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
`test/mpi/run.jl` launches a script through the `mpiexec` that MPI.jl itself
provides:

```sh
# julia --project=<env> test/mpi/run.jl <nranks> <threads> <script> [args...]
julia --project test/mpi/run.jl 4 4 my_program.jl
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

`mpi_rank`/`mpi_ranks` compose with the GPU keyword specifiers (`cuda_gpu`,
`rocm_gpu`, etc.) — see "GPUs with MPI" below — to target a specific rank's
device, e.g. `Dagger.scope(mpi_rank=1, cuda_gpu=1)`.

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

The GPU keyword specifiers ([Scopes](scopes.md) — `cuda_gpu(s)`, `rocm_gpu(s)`,
`metal_gpu(s)`, `intel_gpu(s)`, `cl_device(s)`) are MPI-aware: combine one with
`mpi_rank`/`mpi_ranks` to target a specific rank's device, or — once
`Dagger.accelerate!(:mpi)` is active — omit the rank entirely to span every
rank:

```julia
# Rank 1's first CUDA device
rank1_gpu = Dagger.scope(mpi_rank=1, cuda_gpu=1)

# Every rank's first CUDA device (accelerate!(:mpi) is already active, so no
# `mpi_rank`/`mpi_ranks` is needed)
all_gpus = Dagger.scope(cuda_gpu=1)

A = rand(Float32, 8, 8)
add1!(X) = (X .+= 1; nothing)   # broadcast; scalar indexing is illegal on GPU arrays

nranks = MPI.Comm_size(MPI.COMM_WORLD)
Dagger.with_options(scope=all_gpus) do
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=Dagger.scope(mpi_rank=0, cuda_gpu=1) add1!(InOut(A))
        Dagger.@spawn scope=Dagger.scope(mpi_rank=min(1, nranks-1), cuda_gpu=1) add1!(InOut(A))
    end
end
```

To mix CPU and GPU tasks in one region, make the outer scope a `UnionScope` of
both the CPU and GPU processors the region will use (e.g.
`Dagger.UnionScope(Dagger.scope(mpi_ranks=:), Dagger.scope(cuda_gpu=1))`).

If you need finer control than the keyword specifiers give you (e.g. selecting
by device UUID), you can still enumerate processors and build an `ExactScope`
by hand:

```julia
MPIExt = Base.get_extension(Dagger, :MPIExt)

mpi_procs() = collect(Dagger.get_processors(MPIExt.MPIClusterProc(MPI.COMM_WORLD)))
gpu_proc(r) = first(p for p in mpi_procs()
                    if p.rank == r && p.innerProc isa Dagger.CuArrayDeviceProc)
gpu_scope(r) = Dagger.ExactScope(gpu_proc(r))
```

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

- CPU MPI environment: `test/mpi/env` (deps: `Dagger`, `MPI`, ...).
- GPU MPI environments: `test/cudaenv`, `test/rocmenv`, `test/openclenv`,
  `test/metalenv`, `test/oneapienv` (each adds its backend).
- CPU suite: `test/mpi/cpu.jl`. Shared GPU suite: `test/mpi/gpu_suite.jl`, driven by
  per-backend entry points `test/mpi/cuda.jl`, `test/mpi/rocm.jl`,
  `test/mpi/opencl.jl`, `test/mpi/metal.jl`, `test/mpi/oneapi.jl`.

Run a suite directly through MPI.jl's bundled `mpiexec` (no system MPI needed):

```sh
# CPU suite, 4 ranks, 2 threads each
julia --project=test/mpi/env test/mpi/run.jl 4 2 test/mpi/cpu.jl

# CUDA GPU suite, 2 ranks
julia --project=test/cudaenv test/mpi/run.jl 2 2 test/mpi/cuda.jl
```

## Rank-local task graphs

Under SPMD every rank plans every task. When a phase of a program has a
data-dependent amount of work that differs per rank (building a local tree, say), the
shared graph forces all ranks to plan all of it, and the task counts have to agree.
[`Dagger.with_rank_local`](@ref) runs a block whose tasks only the calling rank plans
and runs; the other ranks never see them:

```julia
sums = Dagger.with_rank_local() do
    # a different number of tasks on every rank is fine here
    fetch.([Dagger.@spawn(sum(local_pieces[i])) for i in 1:length(local_pieces)])
end
```

[`Dagger.per_rank`](@ref)`(f, T)` hands the result over to the shared graph: every
rank runs `f()` as a rank-local graph and gets a vector with one shared chunk per
rank, owned by that rank. `T` is the element type of the result and must be given
by the caller; it is checked against what the task returned.

Rules for data crossing the two graphs:

- A rank-local task may read a chunk of the shared graph that this rank owns, in place.
- A shared task cannot read a rank-local chunk; use `per_rank`. This is an error.
- A rank-local task cannot read a chunk owned by another rank. This is an error.
- A scope `Dagger.scope(mpi_rank=me)` inside a rank-local block means this rank's
  processors of that graph; a scope naming another rank matches nothing.
- The graph is selected through the task-local acceleration, so tasks started with
  `Threads.@spawn` inside the block do not belong to it.

## Group task graphs

[`Dagger.with_group`](@ref)`(f, comm)` is the same idea for a communicator of `k` ranks
(made by the application with `MPI.Comm_split` or `MPI.Comm_create_group`): only its members
plan and run the tasks of the block, with their own task ids, so planning per rank does
not grow with the total number of tasks. Every member runs the same program inside the block.
The first use of a communicator is collective over its members.
[`Dagger.per_group`](@ref)`(f, comm, T)` is `per_rank` on `comm`, and `per_rank` is
`per_group` on the world. Data crosses between groups under the rules of rank-local graphs:
only a process that is a member of both and owns the chunk reads it, in place.

Each group runs a broadcast relay on a duplicate of its communicator. Call
[`Dagger.free_group!`](@ref)`(comm)` (collective, after the group's tasks are done and before
freeing `comm`) to stop it and free the duplicate; a chunk of a freed group cannot be fetched
any more (an error). A group costs a communicator and a relay task, so a group graph is worth
it when many ranks would otherwise plan each other's tasks, not for a few ranks. The ids of a
group's tasks are limited to `2^24` per group: past that the next spawn is an error, so free
the group and start a new one. The first use of a communicator exchanges a small message with
every member on the tag `MPI.tag_ub() - 1` of `comm`; the application must not use that tag on it.
Uniformity checks are off inside groups, and a task of the world cannot read a group chunk.

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
