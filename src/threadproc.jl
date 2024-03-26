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
    # FIXME: Use return type of the call to specialize container
    result = Ref{Any}()
    task = Task() do
        set_tls!(tls)
        TimespanLogging.prof_task_put!(tls.sch_handle.thunk_id.id)
        result[] = @invokelatest f(args...; kwargs...)
        return
    end
    set_task_tid!(task, proc.tid)
    schedule(task)
    try
        fetch(task)
        return result[]
    catch err
        err, frames = Base.current_exceptions(task)[1]
        rethrow(CapturedException(err, frames))
    end
end
get_parent(proc::ThreadProc) = OSProc(proc.owner)
default_enabled(proc::ThreadProc) = true
short_name(proc::ThreadProc) = "W: $(proc.owner), TID: $(proc.tid)"

# TODO: ThreadGroupProc?
