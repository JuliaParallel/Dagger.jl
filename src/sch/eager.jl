const EAGER_INIT = Ref{Bool}(false)
const EAGER_THUNK_CHAN = Channel(typemax(Int))
const EAGER_ID_MAP = Dict{UInt64,Int}()
const EAGER_CONTEXT = Ref{Context}()
const EAGER_STATE = Ref{ComputeState}()

function eager_context()
    if !isassigned(EAGER_CONTEXT)
        EAGER_CONTEXT[] = Context([myid(),workers()...])
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    EAGER_INIT[] && return
    EAGER_INIT[] = true
    ctx = eager_context()
    @async try
        sopts = SchedulerOptions(;allow_errors=true)
        topts = ThunkOptions(;single=1)
        Dagger.compute(ctx, Dagger.delayed(eager_thunk;options=topts)(); options=sopts)
    catch err
        iob = IOContext(IOBuffer(), :color=>true)
        println(iob, "Error in eager scheduler:")
        Base.showerror(iob, err)
        Base.show_backtrace(iob, catch_backtrace())
        println(iob)
        seek(iob.io, 0)
        write(stderr, iob)
    finally
        EAGER_INIT[] = false
    end
end

"Adjusts the scheduler's cached pressure indicators for the specified worker by
the specified amount, and signals the scheduler to try scheduling again if
pressure decreased."
function adjust_pressure!(h::SchedulerHandle, proc::Processor, pressure)
    uid = Dagger.get_tls().sch_uid
    lock(TASK_SYNC) do
        PROC_UTILIZATION[uid][proc][] += pressure
        notify(TASK_SYNC)
    end
    exec!(_adjust_pressure!, h, myid(), proc, pressure)
end
function _adjust_pressure!(ctx, state, task, tid, (pid, proc, pressure))
    state.worker_pressure[pid][proc] += pressure
    if pressure < 0
        put!(state.chan, RescheduleSignal())
    end
    nothing
end

"Allows a thunk to safely wait on another thunk, by temporarily reducing its
effective pressure to 0."
function thunk_yield(f)
    if Dagger.in_thunk()
        h = sch_handle()
        tls = Dagger.get_tls()
        proc = tls.processor
        util = tls.utilization
        adjust_pressure!(h, proc, -util)
        try
            f()
        finally
            adjust_pressure!(h, proc, util)
        end
    else
        f()
    end
end

function eager_thunk()
    h = sch_handle()
    exec!(h) do ctx, state, task, tid, _
        EAGER_STATE[] = state
    end
    tls = Dagger.get_tls()
    # Don't apply pressure from this thunk
    adjust_pressure!(h, tls.processor, -tls.utilization)
    while isopen(EAGER_THUNK_CHAN)
        try
            added_future, future, uid, ref, f, args, opts = take!(EAGER_THUNK_CHAN)
            # preserve inputs until they enter the scheduler
            tid = GC.@preserve args begin
                _args = map(x->x isa Dagger.EagerThunk ? ThunkID(EAGER_ID_MAP[x.uid], x.thunk_ref) : x, args)
                add_thunk!(f, h, _args...; future=future, ref=ref, opts...)
            end
            EAGER_ID_MAP[uid] = tid.id
            put!(added_future, tid.ref)
        catch err
            iob = IOContext(IOBuffer(), :color=>true)
            println(iob, "Error in eager listener:")
            Base.showerror(iob, err)
            Base.show_backtrace(iob, catch_backtrace())
            println(iob)
            seek(iob.io, 0)
            write(stderr, iob)
        end
    end
end

eager_cleanup(t::Dagger.EagerThunkFinalizer) =
    @async eager_cleanup(EAGER_STATE[], t.uid)
function eager_cleanup(state, uid)
    lock(state.lock) do
        if !haskey(EAGER_ID_MAP, uid)
            return
        end
        tid = EAGER_ID_MAP[uid]
        delete!(EAGER_ID_MAP, uid)

        # N.B. cache and errored expire automatically
        delete!(state.thunk_dict, tid)
    end
end

_find_thunk(e::Dagger.EagerThunk) =
    unwrap_weak_checked(EAGER_STATE[].thunk_dict[EAGER_ID_MAP[e.uid]])
