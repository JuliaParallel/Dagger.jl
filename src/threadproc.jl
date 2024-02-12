"""
    ThreadProc <: Processor

Julia CPU (OS) thread, identified by Julia thread ID.
"""
struct ThreadProc <: Processor
    owner::Int
    tid::Int
end
iscompatible(proc::ThreadProc, opts, f, args...) = true
iscompatible_func(proc::ThreadProc, opts, f) = true
iscompatible_arg(proc::ThreadProc, opts, x) = true
function execute!(proc::ThreadProc, @nospecialize(f), @nospecialize(args...); @nospecialize(kwargs...))
    tls = get_tls()
    task = Task() do
        set_tls!(tls)
        TimespanLogging.prof_task_put!(tls.sch_handle.thunk_id.id)
        @invokelatest f(args...; kwargs...)
    end
    set_task_tid!(task, proc.tid)
    schedule(task)
    try
        fetch(task)
    catch err
        @static if VERSION < v"1.7-rc1"
            stk = Base.catch_stack(task)
        else
            stk = Base.current_exceptions(task)
        end
        err, frames = stk[1]
        rethrow(CapturedException(err, frames))
    end
end
get_parent(proc::ThreadProc) = OSProc(proc.owner)
default_enabled(proc::ThreadProc) = true

# TODO: ThreadGroupProc?

