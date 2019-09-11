export OSProc, Context

abstract type Processor end

"""
OS process - contains pid returned by `addprocs`
"""
struct OSProc <: Processor
    pid::Int
end

"""
A context represents a set of processors to use
for a papply operation.
"""
mutable struct Context
    procs::Array{OSProc}
    log_sink::Any
    profile::Bool
    options
end

function Context(xs)
    Context(xs, NoOpLog(), false, nothing) # By default don't log events
end
Context(xs::Array{Int}) = Context(map(OSProc, xs))
Context(;nthreads=Threads.nthreads()) = Context([workers() for i=1:nthreads] |> Iterators.flatten |> collect)
procs(c::Context) = c.procs

"""
Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end
