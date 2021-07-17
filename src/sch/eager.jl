const EAGER_INIT = Ref{Bool}(false)
const EAGER_THUNK_CHAN = Channel(typemax(Int))
const EAGER_THUNK_MAP = Dict{Int,Int}()
const EAGER_CONTEXT = Ref{Context}()

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
    lock(TASK_SYNC) do
        PROC_UTILIZATION[uid][proctype][] += pressure
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
    # Don't apply pressure from this thunk
    adjust_pressure!(h, Dagger.ThreadProc, -util)
    while isopen(EAGER_THUNK_CHAN)
        try
            future, uid, f, args, opts = take!(EAGER_THUNK_CHAN)
            args = map(x->x isa Dagger.EagerThunk ? ThunkID(EAGER_THUNK_MAP[x.uid]) : x, args)
            tid = add_thunk!(f, h, args...; opts...)
            register_future!(h, tid, future)
            EAGER_THUNK_MAP[uid] = tid.id
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
