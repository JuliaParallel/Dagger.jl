const EAGER_INIT = Ref{Bool}(false)
const EAGER_THUNK_CHAN = Ref{Channel{Any}}()
const EAGER_FORCE_KILL = Ref{Bool}(false)
const EAGER_ID_MAP = Dict{UInt64,Int}()
const EAGER_CONTEXT = Ref{Union{Context,Nothing}}(nothing)
const EAGER_STATE = Ref{Union{ComputeState,Nothing}}(nothing)

function eager_context()
    if EAGER_CONTEXT[] === nothing
        EAGER_CONTEXT[] = Context([myid(),workers()...])
    end
    return EAGER_CONTEXT[]
end

function init_eager()
    EAGER_INIT[] && return
    EAGER_INIT[] = true
    EAGER_THUNK_CHAN[] = Channel(typemax(Int))
    ctx = eager_context()
    @async try
        sopts = SchedulerOptions(;allow_errors=true)
        opts = Dagger.Options((;scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)),
                                occupancy=Dict(Dagger.ThreadProc=>0)))
        Dagger.compute(ctx, Dagger.delayed(eager_thunk, opts)();
                       options=sopts)
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
        EAGER_FORCE_KILL[] = true
        close(EAGER_THUNK_CHAN[])
    end
end

"""
Allows a thunk to safely wait on another thunk by temporarily reducing its
effective occupancy to 0, which allows a newly-spawned task to run.
"""
function thunk_yield(f)
    if Dagger.in_thunk()
        h = sch_handle()
        tls = Dagger.get_tls()
        proc = Dagger.thunk_processor()
        proc_istate = proc_states(tls.sch_uid) do states
            states[proc].state
        end
        task_occupancy = tls.task_spec[4]

        # Decrease our occupancy and inform the processor to reschedule
        lock(proc_istate.queue) do _
            proc_istate.proc_occupancy[] -= task_occupancy
            @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
        end
        notify(proc_istate.reschedule)
        try
            # Run the yielding code
            return f()
        finally
            # Wait for processor to have occupancy to run this task
            while true
                ready = lock(proc_istate.queue) do _
                    @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
                    if proc_has_occupancy(proc_istate.proc_occupancy[], task_occupancy)
                        proc_istate.proc_occupancy[] += task_occupancy
                        @assert 0 <= proc_istate.proc_occupancy[] <= typemax(UInt32)
                        return true
                    end
                    return false
                end
                ready && break
                yield()
            end
        end
    else
        return f()
    end
end

function eager_thunk()
    @assert myid() == 1
    h = sch_handle()
    exec!(h) do ctx, state, task, tid, _
        EAGER_STATE[] = state
        nothing
    end
    tls = Dagger.get_tls()
    chan = EAGER_THUNK_CHAN[]
    while isopen(chan)
        try
            added_future, future, uid, ref, f, args, opts = take!(chan)
            # preserve inputs until they enter the scheduler
            tid = GC.@preserve args begin
                _args = map(args) do pos_x
                    pos, x = pos_x
                    if x isa Dagger.EagerThunk
                        return pos => ThunkID(EAGER_ID_MAP[x.uid], x.thunk_ref)
                    elseif x isa Dagger.Chunk
                        return pos => WeakChunk(x)
                    else
                        return pos => x
                    end
                end
                add_thunk!(f, h, _args...; future=future, ref=ref, opts...)
            end
            EAGER_ID_MAP[uid] = tid.id
            put!(added_future, tid.ref)
        catch err
            EAGER_FORCE_KILL[] && break
            iob = IOContext(IOBuffer(), :color=>true)
            println(iob, "Error in eager listener:")
            Base.showerror(iob, err)
            Base.show_backtrace(iob, catch_backtrace())
            println(iob)
            seek(iob.io, 0)
            write(stderr, iob)
        end
    end
    EAGER_STATE[] = nothing
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
