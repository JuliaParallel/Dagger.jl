# Self-scheduling parallel loop: one scheduler entry per worker instead of one per chunk.

"""
    local_scope() -> AbstractScope

The scope of all processors that belong to this process in the current acceleration: the default scope of
[`dynamic_for_async`](@ref), whose workers share one counter and so must all run in
this process. Distributed: the worker's processors. MPI: the processors of this rank when the graph has one rank only; in a
graph shared by several ranks it throws, because a per-rank scope would make the ranks disagree: pass
`scope=Dagger.scope(mpi_rank=r)`.
"""
local_scope() = local_scope(current_acceleration())
local_scope(::Acceleration) = Dagger.scope(worker=myid())

# Hands out index chunks of `1:n` from the shared counter `next` until none are left
function _dynamic_for_worker(next::Threads.Atomic{Int}, f, n::Int, chunk::Int, args...)
    taken = 0
    while true
        lo = Threads.atomic_add!(next, chunk)
        lo >= n && break
        hi = min(lo + chunk, n)
        for i in (lo + 1):hi
            f(i, args...)
        end
        taken += hi - lo
    end
    return taken
end

"""
    loop_worker_scopes(scope, workers) -> scope or Vector{AbstractScope}

How the workers of a self-scheduling loop are bound. A scope is a request for a set of processors; the loop's workers must not
all be free to land on the same thread. Distributed: one exact scope per thread (see the method for the reason). MPI: see the
method in MPIExt.
"""
loop_worker_scopes(scope, workers::Int) = loop_worker_scopes(current_acceleration(), scope, workers)
loop_worker_scopes(::Acceleration, scope, workers::Int) = scope
# Distributed (and plain shared memory): one worker per thread of the process, named exactly. Left to the scheduler, a loop's workers
# are placed by load and then stolen: an idle thread takes the first worker from the queue of its (still waking) owner, the second worker
# waits behind it, and the driver's thread (thread 1) may not steal it, so in a few percent of the loops one thread ran every index
# (seen as 7.6 s instead of 4.0 s steps in an application at 2 threads). Every thread takes part, the driver's too: leaving it out
# cost a worker (4 threads: +27 % per step, 8: +10 %). The loop is asynchronous, so a driver that waits on it runs its worker. A scope that
# is not a set of thread processors of one worker is passed on unchanged.
function loop_worker_scopes(accel::DistributedAcceleration, scope::AbstractScope, workers::Int)
    procs = collect(compatible_processors(accel, scope))
    length(procs) >= 2 || return scope
    all(p -> p isa ThreadProc, procs) || return scope
    owner = first(procs).owner
    all(p -> p.owner == owner, procs) || return scope
    sort!(procs; by=p -> p.tid)
    return AbstractScope[ExactScope(p) for p in procs]
end

# `scope` is one scope for all workers, or a vector with one scope per worker (cycled): a scope that names ONE processor binds its
# worker to that thread, so a vector of exact scopes pins each worker to its own thread.
_worker_scope(scope::AbstractVector, w::Int) = scope[mod1(w, length(scope))]
_worker_scope(scope, ::Int) = scope

"""
    dynamic_for_async(f, n::Integer, args...; chunk=nothing, workers=Threads.nthreads(), scope=nothing) -> Vector{DTask}

Start `f(i, args...)` for every `i` in `1:n` as `workers` self-scheduling tasks and return them.
The result of each task is the number of indices that worker took, so `fetch.(tasks)` shows how
the loop was balanced (the entries add up to `n`).
`args` are ordinary task arguments: a `DTask` or a `Chunk` among them is waited for and
resolved before any worker starts, exactly as for `Dagger.@spawn`, so the loop starts when
its inputs exist and the caller need not wait for them (`f(i)` when there are none).
Each worker takes the next `chunk` indices from a shared atomic counter until none are
left, so the scheduler is entered `workers` times, however many chunks there are; with a
task per chunk, a loop of cheap iterations is dominated by the cost of a task (about
100 us). `chunk` defaults to `cld(n, 4workers)`. Workers that find the counter exhausted
return at once, so asking for more workers than free threads is harmless.

All workers must run in one process (they share the counter): the default scope is the
current worker's processors, and each worker is bound to one thread of it (see
[`loop_worker_scopes`](@ref)); under MPI pass a scope inside one rank, e.g.
`scope=Dagger.scope(mpi_rank=r)`. `scope` may also be a vector with one scope per worker (cycled): a scope naming one
processor binds its worker to that thread. `f` must be safe to call from several threads at
once; the order of iterations is unspecified.
"""
function dynamic_for_async(f, n::Integer, args...; chunk::Union{Nothing,Integer}=nothing,
                           workers::Integer=Threads.nthreads(), scope=nothing)
    n = Int(n)
    workers = max(1, min(Int(workers), n))
    chunk = Int(something(chunk, max(1, cld(n, 4 * workers))))
    chunk >= 1 || throw(ArgumentError("chunk must be positive"))
    scope = scope === nothing ? local_scope() : scope
    scope isa AbstractVector || (scope = loop_worker_scopes(scope, workers))
    next = Threads.Atomic{Int}(0)
    return DTask[Dagger.@spawn(scope=_worker_scope(scope, w), _dynamic_for_worker(next, f, n, chunk, args...)) for w in 1:workers]
end

"""
    dynamic_for(f, n::Integer, args...; kwargs...)

Run `f(i, args...)` for every `i` in `1:n` and wait for all of it; rethrows the first failure.
Returns the number of indices each worker took (see [`dynamic_for_async`](@ref)), whose sum is `n`. Called from inside a task, this blocks that task until the
workers are done and the task keeps its processor meanwhile; from a task, prefer
`dynamic_for_async` and hand the returned tasks to a follow-up task as arguments.
"""
function dynamic_for(f, n::Integer, args...; kwargs...)
    tasks = dynamic_for_async(f, n, args...; kwargs...)
    return Int[fetch(t) for t in tasks]
end
