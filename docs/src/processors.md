# Processors

Dagger contains a flexible mechanism to represent CPUs, GPUs, and other
devices that the scheduler can place user work on. The indiviual devices that
are capable of computing a user operation are called "processors", and are
subtypes of `Dagger.Processor`. Processors are automatically detected by
Dagger at scheduler initialization, and placed in a hierarchy reflecting the
physical (network-, link-, or memory-based) boundaries between processors in
the hierarchy. The scheduler uses the information in this hierarchy to
efficiently schedule and partition user operations.

## Hardware capabilities, topology, and data locality

The processor hierarchy is modeled as a multi-root tree, where each root is an
`OSProc`, which represents a Julia OS process, and the "children" of the root
or some other branch in the tree represent the processors which reside on the
same logical server as the "parent" branch. All roots are connected to each
other directly, in the common case. The processor hierarchy's topology is
automatically detected and elaborated by callbacks in Dagger, which users may
manipulate to add detection of extra processors.

A move between a given pair of processors is implemented as a Julia function
dispatching on the types of each processor, as well as the type of the data
being moved. Users are permitted to define custom move functions to improve
data movement efficiency, perform automatic value conversions, or even make
use of special IPC facilities. Custom processors may also be defined by the
user to represent a processor type which is not automatically detected by
Dagger, such as novel GPUs, special OS process abstractions, FPGAs, etc.

Movement of data between any two processors A and B (from A to B), if not
defined by the user, is decomposed into 3 moves: processor A to OSProc parent
of A, OSProc parent of A to OSProc parent of B, and OSProc parent of B to
processor B. This mechanism uses Julia's Serialization library to serialize and
deserialize data, so data must be serializable for this mechanism to work
properly.

### Future: Hierarchy Generic Path Move

NOTE: This used to be the default move behavior, but was removed because it
wasn't considered helpful, and there were not any processor implementations
that made use of it.

Movement of data between any two processors is decomposable into a sequence of
"moves" between a child and its parent, termed a "generic path move". Movement
of data may also take "shortcuts" between nodes in the tree which are not
directly connected if enabled by libraries or the user, which may make use of
IPC mechanisms to transfer data more directly and efficiently (such as
Infiniband, GPU RDMA, NVLINK, etc.). All data is considered local to some
processor, and may only be operated on by another processor by first doing an
explicit move operation to that processor.

## Processor Selection

By default, Dagger uses the CPU to process work, typically single-threaded per
cluster node. However, Dagger allows access to a wider range of hardware and
software acceleration techniques, such as multithreading and GPUs. These more
advanced (but performant) accelerators are disabled by default, but can easily
be enabled by using Scheduler/Thunk options in the `proclist` field. If
`nothing`, all default processors will be used. If a vector of types, only the
processor types contained in `options.proclist` will be used to compute all or
a given thunk. If a function, it will be called for each processor (with the
processor as the argument) until it returns `true`.

```julia
opts = Dagger.Sch.ThunkOptions(;proclist=nothing) # default behavior
# OR
opts = Dagger.Sch.ThunkOptions(;proclist=[DaggerGPU.CuArrayProc]) # only execute on CuArrayProc
# OR
opts = Dagger.Sch.ThunkOptions(;proclist=(proc)->(proc isa Dagger.ThreadProc && proc.tid == 3)) # only run on ThreadProc with thread ID 3

t = Dagger.@par options=opts sum(X) # do sum(X) on the specified processor
```

## Resource Control

Dagger assumes that a thunk executing on a processor, fully utilizes that
processor at 100%. When this is not the case, you can tell Dagger as much with
`options.procutil`:

```julia
procutil = Dict(
    Dagger.ThreadProc => 4.0, # utilizes 4 CPU threads fully
    DaggerGPU.CuArrayProc => 0.1 # utilizes 10% of a single CUDA GPU
)
```

Dagger will use this information to execute only as many thunks on a given
processor (or set of similar processors) as add up to less than or equal to
`1.0` total utilization. If a thunk is scheduled onto a processor which the
local worker deems as "oversubscribed", it will not execute the thunk until
sufficient resources become available by thunks completing execution.

### GPU Processors

The [DaggerGPU.jl](https://github.com/JuliaGPU/DaggerGPU.jl) package can be
imported to enable GPU acceleration for NVIDIA and AMD GPUs, when available.
The processors provided by that package are not enabled by default, but may be
enabled via `options.proclist` as usual.

### Future: Network Devices and Topology

In the future, users will be able to define network devices attached to a
given processor, which provides a direct connection to a network device on
another processor, and may be used to transfer data between said processors.
Data movement rules will most likely be defined by a similar (or even
identical) mechanism to the current processor move mechanism. The multi-root
tree will be expanded to a graph to allow representing these network devices
(as they may potentially span non-root nodes).

## Redundancy

### Fault Tolerance

Dagger has a single means for ensuring redundancy, which is currently called
"fault tolerance". Said redundancy is only targeted at a specific failure
mode, namely the unexpected exit or "killing" of a worker process in the
cluster. This failure mode often presents itself when running on a Linux and
generating large memory allocations, where the Out Of Memory (OOM) killer
process can kill user processes to free their allocated memory for the Linux
kernel to use. The fault tolerance system mitigates the damage caused by the
OOM killer performing its duties on one or more worker processes by detecting
the fault as a process exit exception (generated by Julia), and then moving
any "lost" work to other worker processes for re-computation.

#### Future: Multi-master, Network Failure Correction, etc.

This single redundancy mechanism helps alleviate a common issue among HPC and
scientific users, however it does little to help when, for example, the master
node exits, or a network link goes down. Such failure modes require a more
complicated detection and recovery process, including multiple master
processes, a distributed and replicated database such as etcd, and
checkpointing of the scheduler to ensure an efficient recovery. Such a system
does not yet exist, but contributions for such a change are desired.

## Dynamic worker pools

Dagger's default scheduler supports modifying the worker pool while the
scheduler is running. This is done by modifying the `Processor`s of the
`Context` supplied to the scheduler at initialization using `addprocs!(ctx, ps)`
and `rmprocs(ctx, ps)` where `ps` can be `Processor`s or just process ids.

An example of when this is useful is in HPC environments where individual jobs
to start up workers are queued so that not all workers are guaranteed to be
available at the same time.

New workers will typically be assigned new tasks as soon as the scheduler sees
them. Removed workers will finish all their assigned tasks but will not be
assigned any new tasks. Note that this makes it difficult to determine when a
worker is no longer in use by Dagger. Contributions to alleviate this
uncertainty are welcome!

Example:

```julia
using Distributed

ps1 = addprocs(2, exeflags="--project")
@everywhere using Distributed, Dagger

# Dummy task to wait for 0.5 seconds and then return the id of the worker
ts = delayed(vcat)((delayed(i -> (sleep(0.5); myid()))(i) for i in 1:20)...)

ctx = Context()
# Scheduler is blocking, so we need a new task to add workers while it runs
job = @async collect(ctx, ts)

# Lets fire up some new workers
ps2 = addprocs(2, exeflags="--project")
@everywhere ps2 using Distributed, Dagger
# New workers are not available until we do this
addprocs!(ctx, ps2)

# Lets hope the job didn't complete before workers were added :)
@show fetch(job) |> unique

# and cleanup after ourselves...
workers() |> rmprocs
```
