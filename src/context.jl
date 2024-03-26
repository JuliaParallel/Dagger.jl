"""
    Context(xs::Vector{OSProc}) -> Context
    Context(xs::Vector{Int}) -> Context

Create a Context, by default adding each available worker.

It is also possible to create a Context from a vector of [`OSProc`](@ref),
or equivalently the underlying process ids can also be passed directly
as a `Vector{Int}`.

Special fields include:
- 'log_sink': A log sink object to use, if any.
- `profile::Bool`: Whether or not to perform profiling with Profile stdlib.
"""
mutable struct Context
    procs::Vector{Processor}
    proc_lock::ReentrantLock
    proc_notify::Threads.Condition
    log_sink::Any
    profile::Bool
    options
end

function Context(procs::Vector{P}=Processor[OSProc(w) for w in procs()];
        proc_lock=ReentrantLock(), proc_notify=Threads.Condition(),
        log_sink=TimespanLogging.NoOpLog(), log_file=nothing, profile=false,
        options=nothing) where {P<:Processor}
    if log_file !== nothing
        @warn "`log_file` is no longer supported\nPlease instead load `GraphViz.jl` and use `render_logs(logs, :graphviz)`."
    end
    Context(procs, proc_lock, proc_notify, log_sink,
            profile, options)
end
Context(xs::Vector{Int}; kwargs...) = Context(map(OSProc, xs); kwargs...)
Context(ctx::Context, xs::Vector=copy(procs(ctx))) = # make a copy
    Context(xs; log_sink=ctx.log_sink,
                profile=ctx.profile, options=ctx.options)

const GLOBAL_CONTEXT = Ref{Context}()
function global_context()
    if !isassigned(GLOBAL_CONTEXT)
        GLOBAL_CONTEXT[] = Context()
    end
    return GLOBAL_CONTEXT[]
end

"""
    lock(f, ctx::Context)

Acquire `ctx.proc_lock`, execute `f` with the lock held, and release the lock
when `f` returns.
"""
Base.lock(f, ctx::Context) = lock(f, ctx.proc_lock)

"""
    procs(ctx::Context)

Fetch the list of procs currently known to `ctx`.
"""
procs(ctx::Context) = lock(ctx) do
    copy(ctx.procs)
end

"""
    addprocs!(ctx::Context, xs)

Add new workers `xs` to `ctx`.

Workers will typically be assigned new tasks in the next scheduling iteration
if scheduling is ongoing.

Workers can be either `Processor`s or the underlying process IDs as `Integer`s.
"""
addprocs!(ctx::Context, xs::AbstractVector{<:Integer}) = addprocs!(ctx, map(OSProc, xs))
function addprocs!(ctx::Context, xs::AbstractVector{<:OSProc})
    lock(ctx) do
        append!(ctx.procs, xs)
    end
    lock(ctx.proc_notify) do
        notify(ctx.proc_notify)
    end
end

"""
    rmprocs!(ctx::Context, xs)

Remove the specified workers `xs` from `ctx`.

Workers will typically finish all their assigned tasks if scheduling is ongoing
but will not be assigned new tasks after removal.

Workers can be either `Processor`s or the underlying process IDs as `Integer`s.
"""
rmprocs!(ctx::Context, xs::AbstractVector{<:Integer}) = rmprocs!(ctx, map(OSProc, xs))
function rmprocs!(ctx::Context, xs::AbstractVector{<:OSProc})
    lock(ctx) do
        filter!(p -> (p ∉ xs), ctx.procs)
    end
    lock(ctx.proc_notify) do
        notify(ctx.proc_notify)
    end
end
