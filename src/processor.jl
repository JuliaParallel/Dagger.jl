import Base.procs
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
    procs::Array{Processor}
    log_sink::Any
    profile::Bool
end

function Context(xs)
    Context(xs, NoOpLog(), false) # By default don't log events
end
Context(xs::Array{Int}) = Context(map(OSProc, xs))
Context() = Context(workers())
procs(c::Context) = c.procs

"""
Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end
