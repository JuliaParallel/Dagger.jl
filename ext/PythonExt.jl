module PythonExt

if isdefined(Base, :get_extension)
    using PythonCall
else
    using ..PythonCall
end

import Dagger
import Dagger: Processor, OSProc, ThreadProc, Chunk
import Distributed: myid

const CPUProc = Union{OSProc, ThreadProc}

struct PythonProcessor <: Processor
    owner::Int
end

Dagger.root_worker_id(proc::PythonProcessor) = proc.owner
Dagger.get_parent(proc::PythonProcessor) = OSProc(proc.owner)
Dagger.default_enabled(::PythonProcessor) = true

Dagger.iscompatible_func(::ThreadProc, opts, ::Type{Py}) = false
Dagger.iscompatible_func(::PythonProcessor, opts, ::Type{Py}) = true
Dagger.iscompatible_arg(::PythonProcessor, opts, ::Type{Py}) = true
Dagger.iscompatible_arg(::PythonProcessor, opts, ::Type{<:PyArray}) = true

Dagger.move(from_proc::CPUProc, to_proc::PythonProcessor, x::Chunk) =
    Dagger.move(from_proc, to_proc, Dagger.move(from_proc, Dagger.get_parent(to_proc), x))
Dagger.move(::CPUProc, ::PythonProcessor, x) = Py(x)
Dagger.move(::CPUProc, ::PythonProcessor, x::Py) = x
Dagger.move(::CPUProc, ::PythonProcessor, x::PyArray) = x
# FIXME: Conversion from Python to Julia

function Dagger.execute!(::PythonProcessor, f, args...; kwargs...)
    @assert f isa Py "Function must be a Python object"
    # Dagger may run this task on any thread (the task is not pinned), but
    # CPython requires that the GIL is held by the calling thread. By default
    # the GIL is held by the thread that initialized Python (the main thread),
    # so calling into Python from a Dagger worker thread without the GIL
    # segfaults. Mirror PythonCall's own approach (see `Base.propertynames`):
    # if the current thread holds the GIL, call directly; otherwise hand the
    # call off to the GIL-holding main thread.
    if PythonCall.C.PyGILState_Check() == 1
        return f(args...; kwargs...)
    else
        return PythonCall.C.on_main_thread(() -> f(args...; kwargs...))
    end
end

function __init__()
    Dagger.add_processor_callback!(:pythonproc) do
        return PythonProcessor(myid())
    end
end

end # module PythonExt
