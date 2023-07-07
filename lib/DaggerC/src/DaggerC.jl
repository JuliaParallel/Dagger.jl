module DaggerC

using Dagger

struct Spec
    id::Cint
    fn::Ptr{Cvoid}
    rettype::Cint
    argtypes::Ptr{Cint}
    nargs::Cint
end

function spawn(spec_ptr::Ptr{Spec}, args_ptrs::Ptr{Ptr{Cvoid}})::Ptr{Cvoid}
    spec = unsafe_load(spec_ptr)
    # FIXME: Options
    opts = Dagger.Options()
    # FIXME: Get proper args and kwargs
    args = []
    for i in 1:spec.nargs
        push!(args, unsafe_load(args_ptrs, i))
    end
    kwargs = NamedTuple()
    task = Dagger.spawn(fn, opts, args...; kwargs...)
    return pointer_from_objref(task)
end
const spawn_jlfunc = Ref(C_NULL)

Base.@ccallable function fetch(task_ptr::Ptr{Cvoid})::Ptr{Cvoid}
    task = unsafe_pointer_to_objref(task_ptr)::Dagger.EagerThunk
    return pointer_from_objref(fetch(task))
end

Base.@ccallable function wait(task_ptr::Ptr{Cvoid})::Cvoid
    task = unsafe_pointer_to_objref(task_ptr)::Dagger.EagerThunk
    wait(task)
    return
end

function __init__()
    spawn_jlfunc[] = @cfunction(spawn, Ptr{Cvoid}, (Ptr{Spec}, Ptr{Ptr{Cvoid}}))
end

end # module DaggerC
