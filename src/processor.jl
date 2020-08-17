export OSProc, Context, addprocs!, rmprocs!

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
    iscompatible(proc::Processor, opts, f, args...) -> Bool

Indicates whether `proc` can execute `f` over `args` given `opts`. `Processor`
subtypes should overload this function to return `true` if and only if it is
essentially guaranteed that `f(args...)` is supported. Additionally,
`iscompatible_func` and `iscompatible_arg` can be overriden to determine
compatibility of `f` and `args` individually. The default implementation
returns `false`.
"""
iscompatible(proc::Processor, opts, f, args...) =
    iscompatible_func(proc, opts, f) &&
    all(x->iscompatible_arg(proc, opts, x), args)
iscompatible_func(proc::Processor, opts, f) = false
iscompatible_arg(proc::Processor, opts, x) = false

"""
    default_enabled(proc::Processor) -> Bool

Returns whether processor `proc` is enabled by default (opt-out). `Processor` subtypes can override this function to make themselves opt-in (default returns `false`).
"""
default_enabled(proc::Processor) = false

"""
    get_processors(proc::Processor) -> Vector{T} where T<:Processor

Returns the full list of processors contained in `proc`, if any. `Processor`
subtypes should overload this function if they can contain sub-processors. The
default method will return a `Vector` containing `proc` itself.
"""
get_processors(proc::Processor) = Processor[proc]

"""
    get_parent(proc::Processor) -> Processor

Returns the parent processor for `proc`. The ultimate parent processor is an
`OSProc`. `Processor` subtypes should overload this to return their most
direct parent.
"""
get_parent

"""
    move(from_proc::Processor, to_proc::Processor, x)

Moves and/or converts `x` such that it's available and suitable for usage on
the `to_proc` processor. This function can be overloaded by `Processor`
subtypes to transport arguments and convert them to an appropriate form before
being used for exection. This is additionally called on thunk results when
moving back to `from_proc` before being serialized over the wire as needed.
The default implementation breaks a single `move` call down into a sequence of
`move` calls, and is not intended to be maximally efficient.
"""
function move(from_proc::Processor, to_proc::Processor, x)
    if from_proc == to_proc
        return x
    end
    @debug "Initiating generic move"
    # Move to remote OSProc
    @debug "(Remote) moving $parent_proc to $grandparent_proc"
    root = get_parent_osproc(from_proc)
    x, parent_proc = remotecall_fetch(move_to_osproc, root.pid, from_proc, x)

    # Move to local OSProc
    remote_proc = parent_proc
    local_proc = OSProc()
    @debug "(Network) moving $remote_proc to $local_proc"
    x = move(remote_proc, local_proc, x)

    # Move to to_proc
    parent_proc = get_parent(to_proc)
    path = Processor[to_proc, parent_proc]
    while parent_proc != local_proc && !(parent_proc isa OSProc)
        parent_proc = get_parent(parent_proc)
        push!(path, parent_proc)
    end
    last_proc = local_proc
    while !isempty(path)
        next_proc = pop!(path)
        @debug "(Local) moving $last_proc to $next_proc"
        x = move(last_proc, next_proc, x)
        last_proc = next_proc
    end
    return x
end
function get_parent_osproc(proc)
    while !(proc isa OSProc)
        proc = get_parent(proc)
    end
    proc
end
function move_to_osproc(parent_proc, x)
    ctx = Context()
    while !(parent_proc isa OSProc)
        grandparent_proc = get_parent(parent_proc)
        x = move(parent_proc, grandparent_proc, x)
        parent_proc = grandparent_proc
    end
    return x, parent_proc
end

"""
    capacity(proc::Processor=OSProc()) -> Int

Returns the total processing capacity of `proc`.
"""
capacity(proc=OSProc()) = length(get_processors(proc))

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
const OSPROC_CACHE = Dict{Int,OSProc}()
OSProc(pid::Int=myid()) = get!(OSPROC_CACHE, pid) do
    remotecall_fetch(get_osproc, pid, pid)
end
function get_osproc(pid::Int)
    proc = OSProc(pid, Dict{Symbol,Any}(), Processor[], Processor[])
    for cb in PROCESSOR_CALLBACKS
        try
            child = Base.invokelatest(cb, proc)
            child !== nothing && push!(proc.children, child)
        catch err
            @error "Error in processor callback" exception=(err,catch_backtrace())
        end
    end
    proc
end
function add_callback!(func)
    push!(Dagger.PROCESSOR_CALLBACKS, func)
    empty!(OSPROC_CACHE)
end
Base.:(==)(proc1::OSProc, proc2::OSProc) = proc1.pid == proc2.pid
iscompatible(proc::OSProc, opts, f, args...) =
    any(child->iscompatible(child, opts, f, args...), proc.children)
iscompatible_func(proc::OSProc, opts, f) =
    any(child->iscompatible_func(child, opts, f), proc.children)
iscompatible_arg(proc::OSProc, opts, args...) =
    any(child->
        all(arg->iscompatible_arg(child, opts, arg), args),
    proc.children)
get_processors(proc::OSProc) =
    vcat((get_processors(child) for child in proc.children)...,)
function choose_processor(from_proc::OSProc, options, f, args)
    if isempty(from_proc.queue)
        for child in from_proc.children
            grandchildren = get_processors(child)
            append!(from_proc.queue, grandchildren)
        end
    end
    @assert !isempty(from_proc.queue)
    for i in 1:length(from_proc.queue)
        proc = popfirst!(from_proc.queue)
        push!(from_proc.queue, proc)
        if !iscompatible(proc, options, f, args...)
            continue
        end
        if default_enabled(proc) && isempty(options.proctypes)
            return proc
        elseif any(p->proc isa p, options.proctypes)
            return proc
        end
    end
    throw(ProcessorSelectionException(options.proctypes, from_proc.queue, f, args))
end
struct ProcessorSelectionException <: Exception
    proctypes::Vector{Type}
    procsavail::Vector{Processor}
    f
    args
end
function Base.show(io::IO, pex::ProcessorSelectionException)
    println(io, "(Worker $(myid())) Exhausted all available processor types!")
    println(io, "  Proctypes: $(pex.proctypes)")
    println(io, "  Procs Available: $(pex.procsavail)")
    println(io, "  Function: $(pex.f)")
    print(io, "  Arguments: $(pex.args)")
end

move(from_proc::OSProc, to_proc::OSProc, x) = x
execute!(proc::OSProc, f, args...) = f(args...)
default_enabled(proc::OSProc) = true

"""
    ThreadProc <: Processor

Julia CPU (OS) thread, identified by Julia thread ID.
"""
struct ThreadProc <: Processor
    owner::Int
    tid::Int
end
iscompatible(proc::ThreadProc, opts, f, args...) = true
iscompatible_func(proc::ThreadProc, opts, f) = true
iscompatible_arg(proc::ThreadProc, opts, x) = true
move(from_proc::OSProc, to_proc::ThreadProc, x) = x
move(from_proc::ThreadProc, to_proc::OSProc, x) = x
@static if VERSION >= v"1.3.0-DEV.573"
    execute!(proc::ThreadProc, f, args...) = fetch(Threads.@spawn begin
        task_local_storage(:processor, proc)
        f(args...)
    end)
else
    # TODO: Use Threads.@threads?
    execute!(proc::ThreadProc, f, args...) = fetch(@async begin
        task_local_storage(:processor, proc)
        f(args...)
    end)
end
get_parent(proc::ThreadProc) = OSProc(proc.owner)
default_enabled(proc::ThreadProc) = true

# TODO: ThreadGroupProc?

"A context represents a set of processors to use for an operation."
mutable struct Context
    procs::Vector{Processor}
    proc_lock::ReentrantLock
    log_sink::Any
    log_file::Union{String,Nothing}
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

Special fields include:
- 'log_sink': A log sink object to use, if any.
- `log_file::Union{String,Nothing}`: Path to logfile. If specified, at
scheduler termination logs will be collected, combined with input thunks, and
written out in DOT format to this location.
- `profile::Bool`: Whether or not to perform profiling with Profile stdlib.
"""
Context(procs::Vector{P}=Processor[OSProc(w) for w in workers()];
        proc_lock=ReentrantLock(), log_sink=NoOpLog(), log_file=nothing,
        profile=false, options=nothing) where {P<:Processor} =
    Context(procs, proc_lock, log_sink, log_file, profile, options)
Context(xs::Vector{Int}) = Context(map(OSProc, xs))
Context() = Context([OSProc(w) for w in workers()])
procs(ctx::Context) = lock(ctx) do
    copy(ctx.procs)
end

"""
    write_event(ctx::Context, event::Event)

Write a log event
"""
function write_event(ctx::Context, event::Event)
    write_event(ctx.log_sink, event)
end

"""
    lock(f, ctx::Context)

Acquire `ctx.proc_lock`, execute `f` with the lock held, and release the lock when `f` returns.
"""
Base.lock(f, ctx::Context) = lock(f, ctx.proc_lock)

"""
    addprocs!(ctx::Context, xs)

Add new workers `xs` to `ctx`.

Workers will typically be assigned new tasks in the next scheduling iteration if scheduling is ongoing.

Workers can be either `Processor`s or the underlying process IDs as `Integer`s.
"""
addprocs!(ctx::Context, xs::AbstractVector{<:Integer}) = addprocs!(ctx, map(OSProc, xs))
addprocs!(ctx::Context, xs::AbstractVector{<:Processor}) = lock(ctx) do
    append!(ctx.procs, xs)
end

"""
    rmprocs!(ctx::Context, xs)

Remove the specified workers `xs` from `ctx`.

Workers will typically finish all their assigned tasks if scheduling is ongoing but will not be assigned new tasks after removal.

Workers can be either `Processor`s or the underlying process IDs as `Integer`s.
"""
rmprocs!(ctx::Context, xs::AbstractVector{<:Integer}) = rmprocs!(ctx, map(OSProc, xs))
rmprocs!(ctx::Context, xs::AbstractVector{<:Processor}) = lock(ctx) do
    filter!(p -> p ∉ xs, ctx.procs)
end

"Gets the current processor executing the current thunk."
thunk_processor() = task_local_storage(:processor)::Processor
