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

"Sets the scheduler's cached pressure indicator for the specified worker."
set_pressure!(h::SchedulerHandle, pid::Int, pressure::Int) =
    exec!(_set_pressure!, h, pid, pressure)
function _set_pressure!(ctx, state, task, tid, (pid, pressure))
    state.worker_pressure[pid] = pressure
end

function eager_thunk()
    h = sch_handle()
    while isopen(EAGER_THUNK_CHAN)
        set_pressure!(h, 1, 0) # HACK: Don't apply pressure from this thunk
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
