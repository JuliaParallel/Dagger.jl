export OSProc, Context

abstract type Processor end

"""
OS process - contains pid returned by `Distributed.workers`
"""
struct OSProc <: Processor
    pid::Int
end

"""
A context represents a set of processors to use for a operation.
"""
mutable struct Context
    procs::Array{OSProc}
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
as a Vector{Int}.
"""
function Context(xs)
    Context(xs, NoOpLog(), false, nothing) # By default don't log events
end
Context(xs::Array{Int}) = Context(map(OSProc, xs))
Context(;nthreads=Threads.nthreads()) = Context([workers() for i=1:nthreads] |> Iterators.flatten |> collect)
procs(ctx::Context) = ctx.procs

"""
    write_event(ctx::Context, event::Event)

Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end
