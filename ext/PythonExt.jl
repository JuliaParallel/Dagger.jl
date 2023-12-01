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

# N.B. We ignore world here because Python doesn't have world ages
function Dagger.execute!(::PythonProcessor, world::UInt64, f, args...; kwargs...)
    @assert f isa Py "Function must be a Python object"
    return f(args...; kwargs...)
end

function __init__()
    Dagger.add_processor_callback!(:pythonproc) do
        return PythonProcessor(myid())
    end
end

end # module PythonExt
