export OSProc, Context, addprocs!, rmprocs!

import Base: @invokelatest

"""
    Processor

An abstract type representing a processing device and associated memory, where
data can be stored and operated on. Subtypes should be immutable, and
instances should compare equal if they represent the same logical processing
device/memory. Subtype instances should be serializable between different
nodes. Subtype instances may contain a "parent" `Processor` to make it easy to
transfer data to/from other types of `Processor` at runtime.
"""
abstract type Processor end

const PROCESSOR_CALLBACKS = Dict{Symbol,Any}()
const OSPROC_PROCESSOR_CACHE = LockedObject(Dict{Int,Set{Processor}}())

add_processor_callback!(func, name::String) =
    add_processor_callback!(func, Symbol(name))
function add_processor_callback!(func, name::Symbol)
    Dagger.PROCESSOR_CALLBACKS[name] = func
    @safe_lock1 OSPROC_PROCESSOR_CACHE cache delete!(cache, myid())
end
delete_processor_callback!(name::String) =
    delete_processor_callback!(Symbol(name))
function delete_processor_callback!(name::Symbol)
    delete!(Dagger.PROCESSOR_CALLBACKS, name)
    @safe_lock1 OSPROC_PROCESSOR_CACHE cache delete!(cache, myid())
end

"""
    execute!(proc::Processor, f, args...; kwargs...) -> Any

Executes the function `f` with arguments `args` and keyword arguments `kwargs`
on processor `proc`. This function can be overloaded by `Processor` subtypes to
allow executing function calls differently than normal Julia.
"""
function execute! end

"""
    iscompatible(proc::Processor, opts, f, Targs...) -> Bool

Indicates whether `proc` can execute `f` over `Targs` given `opts`. `Processor`
subtypes should overload this function to return `true` if and only if it is
essentially guaranteed that `f(::Targs...)` is supported. Additionally,
`iscompatible_func` and `iscompatible_arg` can be overriden to determine
compatibility of `f` and `Targs` individually. The default implementation
returns `false`.
"""
iscompatible(proc::Processor, opts, f, Targs...) =
    iscompatible_func(proc, opts, f) &&
    all(x->iscompatible_arg(proc, opts, x), Targs)
iscompatible_func(proc::Processor, opts, f) = false
iscompatible_arg(proc::Processor, opts, x) = false

"""
    default_enabled(proc::Processor) -> Bool

Returns whether processor `proc` is enabled by default. The default value is
`false`, which is an opt-out of the processor from execution when not
specifically requested by the user, and `true` implies opt-in, which causes the
processor to always participate in execution when possible.
"""
default_enabled(proc::Processor) = false

"""
    get_processors(proc::Processor) -> Set{<:Processor}

Returns the set of processors contained in `proc`, if any. `Processor` subtypes
should overload this function if they can contain sub-processors. The default
method will return a `Set` containing `proc` itself.
"""
get_processors(proc::Processor) = Set{Processor}([proc])

"""
    get_parent(proc::Processor) -> Processor

Returns the parent processor for `proc`. The ultimate parent processor is an
`OSProc`. `Processor` subtypes should overload this to return their most
direct parent.
"""
get_parent

root_worker_id(proc::Processor) = get_parent(proc).pid

"""
    move(from_proc::Processor, to_proc::Processor, x)

Moves and/or converts `x` such that it's available and suitable for usage on
the `to_proc` processor. This function can be overloaded by `Processor`
subtypes to transport arguments and convert them to an appropriate form before
being used for exection. Subtypes of `Processor` wishing to implement efficient
data movement should provide implementations where `x::Chunk`.
"""
move(from_proc::Processor, to_proc::Processor, x) = x

"""
    OSProc <: Processor

Julia CPU (OS) process, identified by Distributed pid. The logical parent of
all processors on a given node, but otherwise does not participate in
computations.
"""
struct OSProc <: Processor
    pid::Int
    function OSProc(pid::Int=myid())
        if !(@safe_lock1 OSPROC_PROCESSOR_CACHE cache haskey(cache, pid))
            procs = remotecall_fetch(get_processor_hierarchy, pid)
            @safe_lock1 OSPROC_PROCESSOR_CACHE cache begin
                cache[pid] = procs
            end
        end
        return new(pid)
    end
end
get_parent(proc::OSProc) = proc
get_processors(proc::OSProc) = @safe_lock1 OSPROC_PROCESSOR_CACHE cache begin
    get(cache, proc.pid, Set{Processor}())
end
children(proc::OSProc) = get_processors(proc)
function get_processor_hierarchy()
    children = Set{Processor}()
    for name in keys(PROCESSOR_CALLBACKS)
        cb = PROCESSOR_CALLBACKS[name]
        try
            child = Base.invokelatest(cb)
            if (child isa Tuple) || (child isa Vector)
                append!(children, child)
            elseif child !== nothing
                push!(children, child)
            end
        catch err
            @error "Error in processor callback: $name" exception=(err,catch_backtrace())
        end
    end
    children
end
Base.:(==)(proc1::OSProc, proc2::OSProc) = proc1.pid == proc2.pid
iscompatible(proc::OSProc, opts, f, args...) =
    any(child->iscompatible(child, opts, f, args...), children(proc))
iscompatible_func(proc::OSProc, opts, f) =
    any(child->iscompatible_func(child, opts, f), children(proc))
iscompatible_arg(proc::OSProc, opts, args...) =
    any(child->
        all(arg->iscompatible_arg(child, opts, arg), args),
    children(proc))

"Returns a very brief `String` representation of `proc`."
short_name(proc::Processor) = string(proc)
short_name(p::OSProc) = "W: $(p.pid)"
