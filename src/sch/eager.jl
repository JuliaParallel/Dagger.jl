const EAGER_INIT = Ref{Bool}(false)
const EAGER_THUNK_CHAN = Channel(typemax(Int))
const EAGER_ID_MAP = Dict{UInt,Int}()
const EAGER_THUNK_MAP = Dict{Int,Thunk}()
const EAGER_CONTEXT = Ref{Context}()
const EAGER_STATE = Ref{ComputeState}()

eager_context() = isassigned(EAGER_CONTEXT) ? EAGER_CONTEXT[] : nothing

function init_eager()
    EAGER_INIT[] && return
    EAGER_INIT[] = true
    if eager_context() === nothing
        EAGER_CONTEXT[] = Context([myid(),workers()...])
    end
    ctx = EAGER_CONTEXT[]
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

"Adjusts the scheduler's cached pressure indicator for the specified worker by
the specified amount."
function adjust_pressure!(h::SchedulerHandle, proctype::Type, pressure)
    uid = Dagger.get_tls().sch_uid
    lock(ACTIVE_TASKS_LOCK) do
        ACTIVE_TASKS[uid][proctype][] += pressure
        notify(TASK_SYNC)
    end
    exec!(_adjust_pressure!, h, myid(), proctype, pressure)
end
function _adjust_pressure!(ctx, state, task, tid, (pid, proctype, pressure))
    state.worker_pressure[pid][proctype] += pressure
    nothing
end

"Allows a thunk to safely wait on another thunk, by temporarily reducing its
effective pressure to 0."
function thunk_yield(f)
    if Dagger.in_thunk()
        h = sch_handle()
        tls = Dagger.get_tls()
        proctype = typeof(tls.processor)
        util = tls.utilization
        adjust_pressure!(h, proctype, -util)
        try
            f()
        finally
            adjust_pressure!(h, proctype, util)
        end
    else
        f()
    end
end

function eager_thunk()
    h = sch_handle()
    util = Dagger.get_tls().utilization
    exec!(h) do ctx, state, task, tid, _
        EAGER_STATE[] = state
    end
    # Don't apply pressure from this thunk
    adjust_pressure!(h, Dagger.ThreadProc, -util)
    while isopen(EAGER_THUNK_CHAN)
        try
            ev, future, uid, f, args, opts = take!(EAGER_THUNK_CHAN)
            # preserve inputs until they enter the scheduler
            tid = GC.@preserve args begin
                args = map(x->x isa Dagger.EagerThunk ? ThunkID(EAGER_ID_MAP[x.uid]) : x, args)
                add_thunk!(f, h, args...; future=future, eager=true, opts...)
            end
            EAGER_ID_MAP[uid] = tid.id
            notify(ev)
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
    tid = EAGER_ID_MAP[uid]
    delete!(EAGER_ID_MAP, uid)
    thunk = EAGER_THUNK_MAP[tid]
    delete!(EAGER_THUNK_MAP, tid)

    lock(state.lock) do
        # N.B. cache and errored expire automatically
        delete!(state.thunk_dict, thunk.id)
    end
end
