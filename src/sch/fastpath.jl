# Lightweight local placement.
#
# `schedule_one!` is written for the general case: candidate processors of several workers, every
# kind of processor, per-candidate capacity checks, user scopes. A task whose compatible
# processors are all CPU threads of *this* process needs none of it: nothing to rank across
# workers, no data to transfer, `has_capacity` always says yes, and every candidate accepts the
# function and its arguments. For those tasks this path takes the processors the general path
# would have used (the cached compatible set of the task's scope) and does only what still
# matters: pick the less loaded of two random candidates (all candidates when there are few),
# reserve its pressure counter, and hand the spec to phase 3.
#
# A task takes the general path whenever one of these does not hold: an option the fast path
# does not model (`proclist`, `single`, `time_util`, `alloc_util`, `occupancy`), a compute or
# result scope that is not the scope the scheduler would build itself, a chunk argument with a
# scope restriction, an acceleration other than `DistributedAcceleration` (MPI plans uniformly),
# a compatible set with any processor that is not a local `ThreadProc` (workers, GPUs, ...), or a
# function or argument type that a thread refuses.

# Pressure counters of one scheduler's processors, aligned with `LocalFast.procs`
struct FastCounters
    uid::UInt64
    refs::Vector{Threads.Atomic{UInt64}}
end

# The compatible set of one (accel, scope) entry, when it qualifies for the fast path
mutable struct LocalFast
    const procs::Vector{Processor}    # all `ThreadProc`s of this process, in the compatible set's order
    const gproc::OSProc
    const pid::Int
    @atomic counters::Union{Nothing,FastCounters}
end

function build_local_fast(entry, accel)
    accel isa Dagger.DistributedAcceleration || return nothing
    compat = entry.compat
    isempty(compat) && return nothing
    me = myid()
    procs = Vector{Processor}()
    for p in compat
        (p isa Dagger.ThreadProc && p.owner == me) || return nothing
        push!(procs, p)
    end
    return LocalFast(procs, OSProc(me), me, nothing)
end

# The compatible-set cache (`COMPAT_PROCS_CACHE`) is one frequency-ordered cache behind one lock, and
# a hit updates the frequency under that lock: every placing thread takes it for every task (4 us at
# 16 threads, 12-29 us at 64). The fast path reads its entries from a snapshot instead: an immutable-
# by-convention dictionary that is replaced (copy, then store) when an entry is added, so a lookup
# takes no lock. It holds the same `CompatEntry`s the cache does, so processor-hierarchy changes
# (`invalidate_compat_procs_cache!`) empty it too. Bounded: once full, new keys go through the cache.
mutable struct FastIndex
    @atomic dict::Dict{Tuple{UInt,UInt},CompatEntry}
end
const FAST_INDEX = FastIndex(Dict{Tuple{UInt,UInt},CompatEntry}())
const FAST_INDEX_LOCK = ReentrantLock()
const FAST_INDEX_MAX = 2048

function invalidate_fast_index!()
    lock(FAST_INDEX_LOCK) do
        @atomic FAST_INDEX.dict = Dict{Tuple{UInt,UInt},CompatEntry}()
    end
    return
end

function fast_compat_entry(accel, scope, procs::Vector{<:Processor})
    key = (objectid(accel), scope_key(scope))
    entry = get(@atomic(FAST_INDEX.dict), key, nothing)
    if entry !== nothing && entry.scope === scope && entry.procs == procs
        return entry::CompatEntry
    end
    entry = compat_entry(accel, scope, procs)
    lock(FAST_INDEX_LOCK) do
        dict = @atomic FAST_INDEX.dict
        if haskey(dict, key) || length(dict) < FAST_INDEX_MAX
            dict = copy(dict)
            dict[key] = entry
            @atomic FAST_INDEX.dict = dict
        end
    end
    return entry
end

# `entry.fast` is `missing` until first asked
function local_fast_of(entry, accel)
    fast = @atomic entry.fast
    if fast === missing
        fast = build_local_fast(entry, accel)
        @atomic entry.fast = fast
    end
    return fast::Union{Nothing,LocalFast}
end

# What the scheduler builds for a task without any scope of its own; computed once
const UNSCOPED_SCOPE = Ref{Union{Nothing,Dagger.AbstractScope}}(nothing)
function unscoped_scope()
    scope = UNSCOPED_SCOPE[]
    if scope === nothing
        scope = constrain(DefaultScope(), AnyScope())
        UNSCOPED_SCOPE[] = scope
    end
    return scope::Dagger.AbstractScope
end

"""
    local_fast_candidates(state, task, options, procs, accel) -> Union{Nothing,Tuple{LocalFast,AbstractScope}}

The fast path's candidate set and the scope the task is scheduled under, or `nothing` when the
task must take the general path (see the file header). Reads only; reserves nothing.
"""
function local_fast_candidates(state, task, options, procs, accel)::Union{Nothing,Tuple{LocalFast,Dagger.AbstractScope}}
    (options.proclist === nothing && options.single === nothing && options.time_util === nothing &&
     options.alloc_util === nothing && options.occupancy === nothing) || return nothing
    accel isa Dagger.DistributedAcceleration || return nothing
    haskey(state.worker_chans, myid()) || return nothing
    exec = options.exec_scope
    if exec !== nothing
        # The scope was fixed by the caller (Datadeps): it is used as is
        fast = local_fast_of(fast_compat_entry(accel, exec, procs), accel)
        fast === nothing && return nothing
        return (fast, exec)
    end
    if options.compute_scope === nothing && options.scope === nothing && options.result_scope === nothing
        for arg in task.inputs
            value = unwrap_weak_checked(Dagger.value(arg))
            # A chunk with a scope restriction narrows the candidates
            value isa Chunk && !(value.scope isa AnyScope) && return nothing
        end
        scope = unscoped_scope()
        fast = local_fast_of(fast_compat_entry(accel, scope, procs), accel)
        fast === nothing && return nothing
        return (fast, scope)
    end
    return nothing
end

function build_fast_counters!(state, fast::LocalFast)
    refs = Vector{Threads.Atomic{UInt64}}(undef, length(fast.procs))
    lock(state.worker_time_pressure)
    try
        wtp = Dagger.payload(state.worker_time_pressure)
        proc_map = get!(wtp, fast.pid) do
            Dict{Processor,Threads.Atomic{UInt64}}()
        end
        for idx in eachindex(fast.procs)
            refs[idx] = get!(proc_map, fast.procs[idx]) do
                Threads.Atomic{UInt64}(UInt64(0))
            end
        end
    finally
        unlock(state.worker_time_pressure)
    end
    counters = FastCounters(state.uid, refs)
    @atomic fast.counters = counters
    return counters
end

@inline function locked_get(lo::LockedObject, key, default)
    lock(lo)
    try
        return get(Dagger.payload(lo), key, default)
    finally
        unlock(lo)
    end
end

# The index of the candidate to run on: what the general path's ranking would put first. The
# counters are read without a lock (they are atomics).
function pick_fast_candidate(refs::Vector{Threads.Atomic{UInt64}})
    np = length(refs)
    np == 1 && return 1
    if np > TWO_CHOICES_MIN
        i1 = rand(1:np)
        i2 = rand(1:np-1)
        i2 >= i1 && (i2 += 1)
        return @inbounds(refs[i2][]) < @inbounds(refs[i1][]) ? i2 : i1
    end
    # Few candidates: the least loaded, a tie broken uniformly at random
    best = typemax(UInt64)
    idx = 1
    nties = 0
    for i in 1:np
        c = @inbounds(refs[i][])
        if c < best
            best = c
            idx = i
            nties = 1
        elseif c == best
            nties += 1
            rand(1:nties) == 1 && (idx = i)
        end
    end
    return idx
end

"""
    local_fast_place!(state, task, options, sig, fast, scope) -> Union{Nothing,Tuple{ScheduleTaskLocation,ScheduleTaskSpec}}

Chooses a processor of `fast` for `task` and reserves its pressure counter. `nothing` (with
nothing reserved) when a thread would refuse the function or an argument: the general path then
decides.
"""
function local_fast_place!(state, task, options, sig, fast::LocalFast, scope)
    procs = fast.procs
    proc1 = @inbounds procs[1]::Dagger.ThreadProc
    # Compatibility depends on the processor's type only, so one thread speaks for all
    inputs = task.inputs
    Dagger.iscompatible_func(proc1, options, chunktype(task.f)) || return nothing
    for idx in 2:length(inputs)
        value = unwrap_weak_checked(Dagger.value(@inbounds inputs[idx]))
        value isa Thunk && return nothing
        Dagger.iscompatible_arg(proc1, options, chunktype(value)) || return nothing
    end

    est_time_util = round(UInt64, locked_get(state.signature_time_cost, sig, UInt64(1000^3)))::UInt64
    est_alloc_util = locked_get(state.signature_alloc_cost, sig, UInt64(0))::UInt64

    counters = @atomic fast.counters
    if counters === nothing || counters.uid != state.uid
        counters = build_fast_counters!(state, fast)
    end
    refs = counters.refs
    idx = pick_fast_candidate(refs)
    counter_ref = @inbounds refs[idx]
    Threads.atomic_add!(counter_ref, est_time_util)
    lock(state.running_pressure_refs)
    try
        Dagger.payload(state.running_pressure_refs)[task.id] = (counter_ref, est_time_util)
    finally
        unlock(state.running_pressure_refs)
    end
    loc = ScheduleTaskLocation(fast.gproc, @inbounds(procs[idx]))
    spec = ScheduleTaskSpec(task, scope, est_time_util, est_alloc_util, typemax(UInt32))
    return (loc, spec)
end
