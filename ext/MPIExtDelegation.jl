# Home-based task delegation (included from MPIExt.jl).
#
# A task spawned with `lend=true` keeps everything the SPMD design derives per rank: its owner, the
# `MPIRef`, the ids and tags, the stubs on other ranks. Only the call of the task's function on the
# owner may be satisfied by another ("idle") rank: the owner ships `(f, args)`, the thief runs it on a
# free thread and ships the value or the exception back, and the owner then goes on exactly as if it
# had run the function itself (status broadcast, result chunk).
#
# Three parts:
#  * `LendPool`: a pure data structure (no MPI). A lendable task takes one of `capacity` slots or
#    waits in a queue; a freed slot goes to the OLDEST queued task, a thief takes the NEWEST. Every
#    transition happens under the pool's lock, so each task is claimed exactly once, by a local slot
#    or by a thief, and a record is filled before it becomes claimable .
#  * `LendState` + service task: one per rank, on a duplicated communicator that only carries lending
#    traffic (it cannot shift world tags or uniform ids). All sends are non-blocking and are progressed
#    by the service task, so it never waits for a peer that may itself be waiting for it. An idle
#    rank (free slots) keeps a request for as many tasks as it has free slots registered at every other
#    rank (`REQ`); a rank without surplus remembers it (lifeline) and answers when a lendable task is queued.
#  * `mpi_lend_run`: the owner side, called in place of the plain local call of the function.

const LEND_QUEUED = 0
const LEND_LOCAL = 1
const LEND_LENT = 2
const LEND_DONE = 3

mutable struct LendRecord
    const uid::UInt64
    const f::Any
    const args::Tuple
    const kwargs::Any
    state::Int
    nolend::Bool
    thief::Int
    failed::Bool
    abandoned::Bool        # its owner was interrupted while it waited: nobody will use the slot or the answer
    result::Any
    const event::Base.Event
end
LendRecord(uid, f, args, kwargs) =
    LendRecord(uid, f, args, kwargs, LEND_QUEUED, false, -1, false, false, nothing, Base.Event())

# Bytes a task's arguments weigh when shipped (only used for the `max_bytes` cap)
function lend_bytes(a)
    a isa AbstractArray && isbitstype(eltype(a)) && return sizeof(a)
    isbitstype(typeof(a)) && return 0
    return Base.summarysize(a)
end
lend_args_bytes(rec) = sum(lend_bytes, rec.args; init=0)

mutable struct LendPool
    const lock::ReentrantLock
    capacity::Int
    free::Int
    const queue::Vector{LendRecord}          # oldest first
    const lent::Dict{UInt64,LendRecord}      # claimed by a thief, result outstanding
    next_uid::UInt64
end
LendPool(capacity::Integer) =
    LendPool(ReentrantLock(), capacity, capacity, LendRecord[], Dict{UInt64,LendRecord}(), UInt64(1))

function lend_pool_new_uid!(pool::LendPool)
    @lock pool.lock begin
        uid = pool.next_uid
        pool.next_uid += 1
        return uid
    end
end

# `true`: a slot was taken, run now. `false`: queued, wait for `rec.event`.
function lend_pool_enter!(pool::LendPool, rec::LendRecord)
    @lock pool.lock begin
        if pool.free > 0
            pool.free -= 1
            rec.state = LEND_LOCAL
            return true
        end
        push!(pool.queue, rec)
        return false
    end
end

# A slot a task ran in is free again: it goes to the oldest queued task, else back to the pool
function lend_pool_release!(pool::LendPool)
    nxt = nothing
    @lock pool.lock begin
        if isempty(pool.queue)
            pool.free += 1
        else
            nxt = popfirst!(pool.queue)
            nxt.state = LEND_LOCAL
        end
    end
    nxt === nothing || notify(nxt.event)
    return
end

# A slot for work that does not wait (a thief running a lent task)
function lend_pool_try_take!(pool::LendPool)
    @lock pool.lock begin
        pool.free > 0 || return false
        pool.free -= 1
        return true
    end
end

# Claim the newest queued task that may be lent (and that `ok` accepts for this thief)
function lend_pool_steal!(pool::LendPool, thief::Int, ok=Returns(true))
    @lock pool.lock begin
        for i in length(pool.queue):-1:1
            rec = pool.queue[i]
            rec.nolend && continue
            ok(rec) || continue
            deleteat!(pool.queue, i)
            rec.state = LEND_LENT
            rec.thief = thief
            pool.lent[rec.uid] = rec
            return rec
        end
    end
    return nothing
end

# Undo a claim (the thief refused, or the task could not be shipped). A slot may have been freed in
# the meantime, in which case the task runs at once.
function lend_pool_requeue!(pool::LendPool, uid::UInt64; nolend::Bool=false)
    rec = nothing
    run = false
    @lock pool.lock begin
        rec = pop!(pool.lent, uid, nothing)
        rec === nothing && return false
        rec.abandoned && return true           # nobody waits for it any more: it is simply dropped
        rec.nolend |= nolend
        rec.thief = -1
        if pool.free > 0
            pool.free -= 1
            rec.state = LEND_LOCAL
            run = true
        else
            rec.state = LEND_QUEUED
            pushfirst!(pool.queue, rec)
        end
    end
    run && notify(rec.event)
    return true
end

# The owner of `rec` was interrupted (the task was cancelled) while it waited for its slot or its answer: leave the
# queue, or give back the slot that was granted in the meantime. A claim that a thief holds is dropped when the thief
# answers or hands it back. Without this the slot would be lost for good.
function lend_pool_abandon!(pool::LendPool, rec::LendRecord)
    release = false
    @lock pool.lock begin
        rec.abandoned = true
        if rec.state == LEND_QUEUED
            i = findfirst(r -> r === rec, pool.queue)
            i === nothing || deleteat!(pool.queue, i)
        elseif rec.state == LEND_LOCAL
            rec.state = LEND_DONE
            release = true
        end
    end
    release && lend_pool_release!(pool)
    return
end

# Wait for the slot or the answer of `rec`
function lend_wait!(pool::LendPool, rec::LendRecord)
    try
        wait(rec.event)
    catch
        lend_pool_abandon!(pool, rec)
        rethrow()
    end
    return
end

# The thief's answer arrived
function lend_pool_complete!(pool::LendPool, uid::UInt64, failed::Bool, value)
    rec = nothing
    @lock pool.lock begin
        rec = pop!(pool.lent, uid, nothing)
        rec === nothing && return nothing
        rec.result = value
        rec.failed = failed
        rec.state = LEND_DONE
    end
    notify(rec.event)
    return rec
end

# Shutdown: nobody will answer the tasks still out
function lend_pool_fail_lent!(pool::LendPool, err)
    recs = LendRecord[]
    @lock pool.lock begin
        for rec in values(pool.lent)
            rec.result = err
            rec.failed = true
            rec.state = LEND_DONE
            push!(recs, rec)
        end
        empty!(pool.lent)
    end
    foreach(rec -> notify(rec.event), recs)
    return length(recs)
end

# ---------------------------------------------------------------------------------------
# Protocol

const LEND_TAG = 1
const LEND_REQ = UInt8(1)      # (REQ, thief, delta)      a rank with free slots asks for `delta` more tasks
const LEND_GRANT = UInt8(2)    # (GRANT, uid, home, bytes)   victim -> thief: run (f, args, kwargs)
const LEND_RETURN = UInt8(3)   # (RETURN, uid)            thief -> victim: no room, run it yourself
const LEND_RESULT = UInt8(4)   # (RESULT, uid, bytes)
const LEND_FAIL = UInt8(5)     # (FAIL, uid, bytes)       the exception, serialized

mutable struct LendState
    const comm::MPI.Comm              # duplicate, lending traffic only
    const parent::MPI.Comm
    const pool::LendPool
    const running::Threads.Atomic{Bool}
    relay::Union{Task,Nothing}
    const plock::ReentrantLock
    const pending::Vector{Tuple{MPI.Request,Vector{UInt8}}}   # sends in flight (progressed by the service)
    const wlock::ReentrantLock              # guards `waiters` (the service and queuing tasks serve them)
    const waiters::Vector{Tuple{Int,Int}}   # (thief, tasks still wanted)
    const owed::Vector{Int}                 # tasks asked of each peer and not yet delivered, service task only
    const n_lent::Threads.Atomic{Int}       # lent tasks whose answer arrived
    const n_failed::Threads.Atomic{Int}
    const n_returned::Threads.Atomic{Int}   # claims a thief handed back
    const n_stolen::Threads.Atomic{Int}     # tasks of other ranks run here
    const n_local::Threads.Atomic{Int}
    const max_bytes::Int                    # a task whose arguments are larger is not lent
end

const LEND_STATES = LockedObject(Dict{MPI.Comm,LendState}())
const LEND_ENABLED = Ref(false)   # fast check for the per-task path

lend_state_for(comm::MPI.Comm) = lock(LEND_STATES) do states
    get(states, comm, nothing)
end

function lend_encode(msg)
    io = IOBuffer()
    Serialization.serialize(io, msg)
    return take!(io)
end

# Non-blocking send; the service task progresses and releases it. Nothing is sent once delegation is being
# stopped: the communicator is about to be freed, and whoever waits for the answer is failed by `lend_stop!`.
function lend_send!(st::LendState, dest::Integer, msg)
    st.running[] || return
    buf = lend_encode(msg)
    @lock st.plock begin
        st.running[] || return
        req = MPI.Isend(buf, st.comm; dest=Int(dest), tag=LEND_TAG)
        push!(st.pending, (req, buf))
    end
    return
end

function lend_progress_sends!(st::LendState)
    isempty(st.pending) && return
    @lock st.plock begin
        i = 1
        while i <= length(st.pending)
            if MPI.Test(st.pending[i][1])
                deleteat!(st.pending, i)
            else
                i += 1
            end
        end
    end
    return
end

# Victim: hand queued tasks to waiting thieves, round robin. Called by the service task and by a task
# that has just queued itself (the service may not get a thread while every thread runs a task).
function lend_serve_waiters!(st::LendState)
    did = false
    me = MPI.Comm_rank(st.comm)
    while true
        thief = 0
        want = 0
        @lock st.wlock begin
            isempty(st.waiters) && return did
            thief, want = popfirst!(st.waiters)
        end
        rec = lend_pool_steal!(st.pool, thief, st.max_bytes == typemax(Int) ? Returns(true) :
                                               r -> lend_args_bytes(r) <= st.max_bytes)
        if rec === nothing
            lend_add_waiter!(st, thief, want)
            return did
        end
        enc = try
            lend_encode((rec.f, rec.args, rec.kwargs))
        catch
            nothing
        end
        if enc === nothing
            # not serializable: it has to run here
            lend_pool_requeue!(st.pool, rec.uid; nolend=true)
            lend_add_waiter!(st, thief, want)
            continue
        end
        want > 1 && lend_add_waiter!(st, thief, want - 1; last=true)
        lend_send!(st, thief, (LEND_GRANT, rec.uid, me, enc))
        did = true
    end
end

# Add to what the thief has registered (`last`: at the back of the round robin, else at the front). A waiter
# put back after a failed steal merges with whatever a newer request added meanwhile.
function lend_add_waiter!(st::LendState, thief::Int, want::Int; last::Bool=false)
    @lock st.wlock begin
        idx = findfirst(w -> w[1] == thief, st.waiters)
        if idx !== nothing
            st.waiters[idx] = (thief, st.waiters[idx][2] + want)
        elseif last
            push!(st.waiters, (thief, want))
        else
            pushfirst!(st.waiters, (thief, want))
        end
    end
    return
end

# Thief: keep every peer's registered demand at the number of free slots. `owed[peer]` is what that peer
# still owes us, so a slot that frees up asks for one more at once and does not wait for an earlier
# request to be answered (measured: one outstanding request per peer capped a 4 x 16 run at about 50
# lent tasks per second, 80-110 of 512).
function lend_issue_requests!(st::LendState)
    pool = st.pool
    free = pool.free
    free > 0 || return false
    sz = MPI.Comm_size(st.comm)
    sz == 1 && return false
    me = MPI.Comm_rank(st.comm)
    did = false
    for k in 1:(sz-1)
        peer = mod(me + k, sz)
        delta = free - st.owed[peer+1]
        delta > 0 || continue
        lend_send!(st, peer, (LEND_REQ, me, delta))
        st.owed[peer+1] += delta
        did = true
    end
    return did
end

# Thief: run a lent task on a free thread and answer. Whatever happens, the home gets an answer
# (an error on a detached task would be a hang on the owner).
function lend_job(st::LendState, home::Int, uid::UInt64, payload::Vector{UInt8})
    msg = try
        f, args, kwargs = Serialization.deserialize(IOBuffer(payload))
        v = @invokelatest f(args...; kwargs...)
        (LEND_RESULT, uid, lend_encode(v))
    catch err
        wire = try
            mpi_wire_exception(CapturedException(err, catch_backtrace()))
        catch
            ErrorException("task lent from rank $home failed")
        end
        (LEND_FAIL, uid, lend_encode(wire))
    end
    try
        lend_send!(st, home, msg)
    finally
        lend_pool_release!(st.pool)
    end
    return
end

function lend_start_job!(st::LendState, home::Int, uid::UInt64, payload::Vector{UInt8})
    t = Task(() -> lend_job(st, home, uid, payload))
    # N.B. `sticky = false` alone is not enough: a Task made by a task that is bound to a thread
    # (the one that calls `enable_delegation!`, the service) starts on that thread too
    Dagger.set_task_migratable!(t)
    Dagger.clear_task_scope!(t)
    schedule(t)
    Dagger.Sch.errormonitor_tracked("mpi_lend_job", t)
    return
end

function lend_handle!(st::LendState, src::Int, msg)
    kind = msg[1]::UInt8
    if kind == LEND_REQ
        _, thief, want = msg
        lend_add_waiter!(st, Int(thief), Int(want); last=true)
    elseif kind == LEND_GRANT
        _, uid, home, payload = msg
        st.owed[src+1] = max(st.owed[src+1] - 1, 0)
        if lend_pool_try_take!(st.pool)
            Threads.atomic_add!(st.n_stolen, 1)
            lend_start_job!(st, Int(home), uid, payload)
        else
            # no room any more: give it back, and drop the request that sent it
            st.owed[src+1] = 0
            lend_send!(st, src, (LEND_RETURN, uid))
        end
    elseif kind == LEND_RETURN
        uid = msg[2]
        Threads.atomic_add!(st.n_returned, 1)
        @lock st.wlock filter!(w -> w[1] != src, st.waiters)
        lend_pool_requeue!(st.pool, uid)
    elseif kind == LEND_RESULT || kind == LEND_FAIL
        _, uid, payload = msg
        failed = kind == LEND_FAIL
        value = try
            Serialization.deserialize(IOBuffer(payload))
        catch err
            failed = true
            err
        end
        if lend_pool_complete!(st.pool, uid, failed, value) !== nothing
            Threads.atomic_add!(st.n_lent, 1)
            failed && Threads.atomic_add!(st.n_failed, 1)
        end
    end
    return
end

const LEND_SPIN_ITERS = 512
const LEND_IDLE_SLEEP = 2e-4

function lend_service_loop(st::LendState)
    comm = st.comm
    rank = MPI.Comm_rank(comm)
    idle = 0
    may_backoff = Threads.nthreads() > 1
    while st.running[]
        try
            MPI.Finalized() && break
            lend_progress_sends!(st)
            got, msg, stat = MPI.Improbe(MPI.ANY_SOURCE, MPI.ANY_TAG, comm, MPI.Status)
            if got
                idle = 0
                src = Int(MPI.Get_source(stat))
                count = MPI.Get_count(stat, UInt8)
                buf = Array{UInt8}(undef, count)
                req = MPI.Imrecv!(MPI.Buffer(buf), msg)
                __wait_for_request(req, comm, rank, src, LEND_TAG, "lend_service", "recv")
                try
                    lend_handle!(st, src, Serialization.deserialize(IOBuffer(buf)))
                catch err
                    @error "MPI delegation: could not handle a message" exception=(err, catch_backtrace())
                end
                lend_serve_waiters!(st)
                continue
            end
            if lend_serve_waiters!(st) | lend_issue_requests!(st)
                idle = 0
                continue
            end
            idle += 1
            if idle <= LEND_SPIN_ITERS || !may_backoff
                yield()
            else
                Libc.systemsleep(LEND_IDLE_SLEEP)
                yield()
            end
        catch err
            (st.running[] && !MPI.Finalized()) || break
            rethrow(err)
        end
    end
    return
end

function Dagger.enable_delegation!(accel::MPIAcceleration; slots::Integer=max(Threads.nthreads(:default) - 1, 1),
                                   max_bytes::Integer=typemax(Int))
    slots >= 1 || throw(ArgumentError("slots must be positive"))
    lend_state_for(accel.comm) === nothing || return nothing
    # Collective, like every other communicator the backend creates
    comm = MPI.Comm_dup(accel.comm)
    sz = MPI.Comm_size(comm)
    st = LendState(comm, accel.comm, LendPool(slots), Threads.Atomic{Bool}(true), nothing,
                   ReentrantLock(), Tuple{MPI.Request,Vector{UInt8}}[], ReentrantLock(), Tuple{Int,Int}[],
                   zeros(Int, sz), (Threads.Atomic{Int}(0) for _ in 1:5)...,
                   Int(max_bytes))
    lock(LEND_STATES) do states
        states[accel.comm] = st
    end
    LEND_ENABLED[] = true
    t = Task(() -> lend_service_loop(st))
    # N.B. `sticky = false` alone is not enough: a Task made by a task that is bound to a thread
    # (the one that calls `enable_delegation!`, the service) starts on that thread too
    Dagger.set_task_migratable!(t)
    Dagger.clear_task_scope!(t)
    schedule(t)
    Dagger.Sch.errormonitor_tracked("mpi_lend_service", t)
    st.relay = t
    atexit(() -> lend_stop!(accel.comm))
    lend_warmup!(st)
    return nothing
end

# Lend one trivial task to ourselves through the real path (grant, job, result), so that the first real
# lend does not compile it: measured at 4 ranks x 16 threads, the first round of a process took 2.3-2.6x
# the ideal and the warm rounds 1.2x. Self-addressed, hence not collective. The counters are reset after.
function lend_warmup!(st::LendState)
    me = MPI.Comm_rank(st.comm)
    rec = LendRecord(lend_pool_new_uid!(st.pool), identity, (1,), (;))
    @lock st.pool.lock push!(st.pool.queue, rec)
    # N.B. Not through the waiters: another rank's request may already be registered and would get the record
    claimed = lend_pool_steal!(st.pool, me)
    claimed === rec || error("lend warm-up: claimed the wrong record")
    lend_send!(st, me, (LEND_GRANT, rec.uid, me, lend_encode((rec.f, rec.args, rec.kwargs))))
    lend_serve_waiters!(st)      # compile only (nobody has to be waiting)
    deadline = time() + 30
    while rec.state != LEND_DONE && time() < deadline
        sleep(0.001)
    end
    if rec.state != LEND_DONE
        @warn "MPI delegation: the self-addressed warm-up task did not come back"
        lend_pool_requeue!(st.pool, rec.uid; nolend=true)
        @lock st.pool.lock filter!(r -> r !== rec, st.pool.queue)
    end
    for c in (st.n_lent, st.n_failed, st.n_returned, st.n_stolen, st.n_local)
        c[] = 0
    end
    return nothing
end

# Sends that never completed because their receiver stopped: MPI may still read the buffer, so keep it
const LEND_ORPHAN_SENDS = LockedObject(Vector{Any}())

# `free`: also free the communicator, which is collective (`disable_delegation!`); at exit it is not
function lend_stop!(comm::MPI.Comm; free::Bool=false)
    st = lock(LEND_STATES) do states
        s = get(states, comm, nothing)
        s === nothing || delete!(states, comm)
        isempty(states) && (LEND_ENABLED[] = false)
        s
    end
    st === nothing && return nothing
    st.running[] = false
    t = st.relay
    if t !== nothing && t !== current_task()
        try
            wait(t)
        catch
        end
    end
    lend_pool_fail_lent!(st.pool, ErrorException("MPI delegation was stopped while a task was lent"))
    # `lend_send!` takes `plock` and checks `running` under it, so nothing is sent after this
    @lock st.plock begin
        lend_progress_sends!(st)
        isempty(st.pending) || lock(LEND_ORPHAN_SENDS) do orphans
            append!(orphans, st.pending)
        end
        empty!(st.pending)
    end
    free && !MPI.Finalized() && MPI.free(st.comm)
    return nothing
end

function Dagger.disable_delegation!(accel::MPIAcceleration)
    lend_state_for(accel.comm) === nothing && return nothing
    # Nobody may still be lending to us: wait for every rank to get here, cooperatively
    # (a blocking barrier parks the thread the broadcast relay needs)
    req = MPI.Ibarrier(accel.comm)
    __wait_for_request(req, accel.comm, MPI.Comm_rank(accel.comm), -1, 0, "disable_delegation", "barrier")
    lend_stop!(accel.comm; free=true)
    return nothing
end

function Dagger.delegation_stats(accel::MPIAcceleration)
    st = lend_state_for(accel.comm)
    st === nothing && return nothing
    return (lent=st.n_lent[], failed=st.n_failed[], returned=st.n_returned[],
            stolen=st.n_stolen[], local_=st.n_local[])
end

# ---------------------------------------------------------------------------------------
# Owner side

# The state to lend with, or `nothing`: the task did not ask, delegation is off, or the task is not one
# whose execution may leave its rank. Called for every task that runs on an MPI processor, so the
# common case (`lend` unset) is one field read.
function mpi_lend_state(proc::MPIProcessor, f, opts::Options)
    opts.lend === true || return nothing
    LEND_ENABLED[] || return nothing
    opts.tag === nothing || return nothing        # Datadeps tasks are never lent
    proc.innerProc isa ThreadProc || return nothing
    proc.rank == MPI.Comm_rank(proc.comm) || return nothing
    (f isa Function || f isa Type) || return nothing
    f === move! && return nothing
    # A scope that names processors is a promise to run there
    for scope in (opts.scope, opts.compute_scope)
        (scope === nothing || scope === Dagger.DEFAULT_SCOPE_INSTANCE || scope isa Dagger.AnyScope) || return nothing
    end
    return lend_state_for(proc.comm)
end

mpi_lend_plain_arg(a) = !(a isa Union{Chunk,WeakChunk,Dagger.DTask,Thunk})

# Called in place of the plain local call on the owner
function mpi_lend_run(st::LendState, proc::MPIProcessor, f, raw_args::Tuple, kwargs)
    lendable = all(mpi_lend_plain_arg, raw_args)
    pool = st.pool
    if lendable
        rec = LendRecord(lend_pool_new_uid!(pool), f, raw_args, kwargs)
        if !lend_pool_enter!(pool, rec)
            # queued: an idle rank may be waiting for exactly this
            lend_serve_waiters!(st)
            lend_wait!(pool, rec)
            if rec.state == LEND_DONE
                rec.failed && throw(rec.result)
                return rec.result
            end
        end
    else
        # arguments that cannot travel: take a slot like any other lendable task, but never lend
        rec = LendRecord(lend_pool_new_uid!(pool), f, raw_args, kwargs)
        rec.nolend = true
        if !lend_pool_enter!(pool, rec)
            lend_wait!(pool, rec)
        end
    end
    Threads.atomic_add!(st.n_local, 1)
    try
        return mpi_gated_execute!(proc.innerProc, f, raw_args...; kwargs...)
    finally
        lend_pool_release!(pool)
    end
end
