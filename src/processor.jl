export OSProc, Context

"""
    Processor

An abstract type representing a processing device and associated memory, where
data can be stored and operated on. Subtypes should be immutable, and
instances should compare equal if they represent the same logical processing
device/memory. Subtype instances should be serializable between different
nodes. Subtype instances may contain a pointer to a "parent" `Processor` to
make it easy to transfer data to/from other types of `Processor` at runtime.
"""
abstract type Processor end

const PROCESSOR_CALLBACKS = []

"""
    execute!(proc::Processor, f, args...) -> Any

Executes the function `f` with arguments `args` on processor `proc`. This
function can be overloaded by `Processor` subtypes to allow executing function
calls differently than normal Julia.
"""
execute!

"""
    iscompatible(proc::Processor, opts, x) -> Bool

Indicates whether `proc` can execute `x` given `opts`. `Processor` subtypes
should overload this function to return `true` if and only if it is
essentially guaranteed that `x` is supported. The default is to return
`false`.
"""
iscompatible(proc::Processor, opts, x) = false

"""
    get_processors(proc::Processor) -> Vector{T} where T<:Processor

Returns the full list of processors contained in `proc`, if any. `Processor`
subtypes should overload this function if they can contain sub-processors. The
default method will return a `Vector` containing `proc` itself.
"""
get_processors(proc::Processor) = Processor[proc]

"""
    move(from_proc::Processor, to_proc::Processor, x)

Moves and/or converts `x` such that it's available and suitable for usage on
the `to_proc` processor. This function can be overloaded by `Processor`
subtypes to transport arguments and convert them to an appropriate form before
being used for exection.
"""
move

"""
    OSProc <: Processor

Julia CPU (OS) process, identified by Distributed pid. Executes thunks when
threads or other "children" processors are not available, and/or the user has
not opted to use those processors.
"""
struct OSProc <: Processor
    pid::Int
    attrs::Dict{Symbol,Any}
    children::Vector{Processor}
    queue::Vector{Processor}
end
OSProc(pid::Int=myid()) = remotecall_fetch(get_osproc, pid, pid)
function get_osproc(pid::Int)
    proc = OSProc(pid, Dict{Symbol,Any}(), Processor[], Processor[])
    for cb in PROCESSOR_CALLBACKS
        try
            child = cb(proc)
            push!(proc.children, child)
        catch err
            @error "Error in processor callback" exception=(err,catch_backtrace())
        end
    end
    proc
end
Base.:(==)(proc1::OSProc, proc2::OSProc) = proc1.pid == proc2.pid
function iscompatible(proc::OSProc, opts, x)
    for child in proc.children
        if iscompatible(child, opts, x)
            return true
        end
    end
    return true
end
get_processors(proc::OSProc) =
    vcat(get_processors(child) for child in proc.children)
function choose_processor(from_proc::OSProc, options, f, args)
    if isempty(from_proc.queue)
        for child in from_proc.children
            grandchildren = get_processors(child)
            append!(from_proc.queue, grandchildren)
        end
    end
    @assert !isempty(from_proc.queue)
    while true
        proc = popfirst!(from_proc.queue)
        push!(from_proc.queue, proc)
        if !all(x->iscompatible(proc, options, x), args)
            continue
        end
        if isempty(options.proctypes)
            return proc
        elseif any(p->proc isa p, options.proctypes)
            return proc
        end
    end
end
move(ctx, from_proc::OSProc, to_proc::OSProc, x) = x
execute!(proc::OSProc, f, args...) = f(args...)

"""
    ThreadProc <: Processor

Julia CPU (OS) thread, identified by Julia thread ID.
"""
struct ThreadProc <: Processor
    tid::Int
end
iscompatible(proc::ThreadProc, opts, x) = true
move(ctx, from_proc::OSProc, to_proc::ThreadProc, x) = x
move(ctx, from_proc::ThreadProc, to_proc::OSProc, x) = x
@static if VERSION >= v"1.3.0-DEV.573"
    execute!(proc::ThreadProc, f, args...) = fetch(Threads.@spawn f(args...))
else
    # TODO: Use Threads.@threads?
    execute!(proc::ThreadProc, f, args...) = fetch(@async f(args...))
end

# TODO: ThreadGroupProc?

"A context represents a set of processors to use for an operation."
mutable struct Context
    procs::Vector{Processor}
    log_sink::Any
    profile::Bool
    options
end

"""
    Context(;nthreads=Threads.nthreads()) -> Context
    Context(xs::Vector{OSProc}) -> Context
    Context(xs::Vector{Int}) -> Context

Create a Context, by default adding each available worker once from
every available thread. Use the `nthreads` keyword to use a different
number of threads.

It is also possible to create a Context from a vector of [`OSProc`](@ref),
or equivalently the underlying process ids can also be passed directly
as a `Vector{Int}`.
"""
function Context(xs)
    Context(xs, NoOpLog(), false, nothing) # By default don't log events
end
Context(xs::Vector{Int}) = Context(map(OSProc, xs))
function Context()
    procs = [OSProc(w) for w in workers()]
    Context(procs)
end
procs(ctx::Context) = ctx.procs

"""
    write_event(ctx::Context, event::Event)

Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end
