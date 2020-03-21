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
    OSProc <: Processor

Julia CPU (OS) process, identified by Distributed pid.
"""
struct OSProc <: Processor
    pid::Int
    attrs::Dict{Symbol,Any}
    children::Vector{Processor}
end
OSProc(pid::Int=myid()) = OSProc(pid, Dict{Symbol,Any}(), Processor[])
function get_osproc()
    proc = OSProc()
    proc.attrs[:threads] = Threads.nthreads()
    for cb in PROCESSOR_CALLBACKS
        try
            cb(proc)
        catch err
            @error "Error in processor callback" exception=(err,catch_backtrace())
        end
    end
    proc
end

"""
    execute!(proc::Processor, f, args...) -> Any

Executes the function `f` with arguments `args` on processor `proc`. This
function can be overloaded by `Processor` subtypes to allow executing function
calls differently than normal Julia.
"""
execute!(proc::Processor, f, args...) = f(args...)

"""
    convert_arg(from_proc::Processor, to_proc::Processor, x)

Converts `x` such that it's suitable for usage on a `to_proc` processor. This
function can be overloaded by `Processor` subtypes to convert arguments to an
appropriate form before being used for exection.
"""
function convert_arg(from_proc::Processor, to_proc::Processor, x)
    @show typeof(from_proc)
    @show typeof(to_proc)
    x
end

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
    procs = OSProc[remotecall_fetch(get_osproc, w) for w in workers()]
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
