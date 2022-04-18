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

const PROCESSOR_CALLBACKS = Dict{Symbol,Any}()

"""
    execute!(proc::Processor, f, args...) -> Any

Executes the function `f` with arguments `args` on processor `proc`. This
function can be overloaded by `Processor` subtypes to allow executing function
calls differently than normal Julia.
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
being used for exection. Subtypes of `Processor` wishing to implement efficient
data movement should provide implementations where `x::Chunk`.
"""
move(from_proc::Processor, to_proc::Processor, x) = x

"""
    capacity(proc::Processor=OSProc()) -> Int

Returns the total processing capacity of `proc`.
"""
capacity(proc=OSProc()) = length(get_processors(proc))
capacity(proc, ::Type{T}) where T =
    length(filter(x->x isa T, get_processors(proc)))

"""
    OSProc <: Processor

Julia CPU (OS) process, identified by Distributed pid. The logical parent of
all processors on a given node, but otherwise does not participate in
computations.
"""
struct OSProc <: Processor
    pid::Int
    function OSProc(pid::Int=myid())
        get!(OSPROC_CACHE, pid) do
            remotecall_fetch(get_proc_hierarchy, pid)
        end
        new(pid)
    end
end
const OSPROC_CACHE = Dict{Int,Vector{Processor}}()
get_parent(proc::OSProc) = proc
children(proc::OSProc) = get(OSPROC_CACHE, proc.pid, Processor[])
function get_proc_hierarchy()
    children = Processor[]
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
add_processor_callback!(func, name::String) =
    add_processor_callback!(func, Symbol(name))
function add_processor_callback!(func, name::Symbol)
    Dagger.PROCESSOR_CALLBACKS[name] = func
    empty!(OSPROC_CACHE)
end
delete_processor_callback!(name::String) =
    delete_processor_callback!(Symbol(name))
function delete_processor_callback!(name::Symbol)
    delete!(Dagger.PROCESSOR_CALLBACKS, name)
    empty!(OSPROC_CACHE)
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
function get_processors(proc::OSProc)
    procs = Processor[]
    for child in children(proc)
        append!(procs, get_processors(child))
    end
    procs
end

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
function execute!(proc::ThreadProc, @nospecialize(f), @nospecialize(args...))
    tls = get_tls()
    task = Task() do
        set_tls!(tls)
        prof_task_put!(tls.sch_handle.thunk_id.id)
        f(args...)
    end
    ret = ccall(:jl_set_task_tid, Cint, (Any, Cint), task, proc.tid-1)
    if ret == 0
        error("jl_set_task_tid == 0")
    end
    @assert Threads.threadid(task) == proc.tid
    schedule(task)
    try
        fetch(task)
    catch err
        @static if VERSION < v"1.7-rc1"
            stk = Base.catch_stack(task)
        else
            stk = Base.current_exceptions(task)
        end
        err, frames = stk[1]
        rethrow(CapturedException(err, frames))
    end
end
get_parent(proc::ThreadProc) = OSProc(proc.owner)
default_enabled(proc::ThreadProc) = true

# TODO: ThreadGroupProc?

"""
    Context(xs::Vector{OSProc}) -> Context
    Context(xs::Vector{Int}) -> Context

Create a Context, by default adding each available worker.

It is also possible to create a Context from a vector of [`OSProc`](@ref),
or equivalently the underlying process ids can also be passed directly
as a `Vector{Int}`.

Special fields include:
- 'log_sink': A log sink object to use, if any.
- `log_file::Union{String,Nothing}`: Path to logfile. If specified, at
scheduler termination, logs will be collected, combined with input thunks, and
written out in DOT format to this location.
- `profile::Bool`: Whether or not to perform profiling with Profile stdlib.
"""
mutable struct Context
    procs::Vector{Processor}
    proc_lock::ReentrantLock
    proc_notify::Threads.Condition
    log_sink::Any
    log_file::Union{String,Nothing}
    profile::Bool
    options
end

Context(procs::Vector{P}=Processor[OSProc(w) for w in workers()];
        proc_lock=ReentrantLock(), proc_notify=Threads.Condition(),
        log_sink=NoOpLog(), log_file=nothing, profile=false,
        options=nothing) where {P<:Processor} =
    Context(procs, proc_lock, proc_notify, log_sink, log_file,
            profile, options)
Context(xs::Vector{Int}; kwargs...) = Context(map(OSProc, xs); kwargs...)
Context(ctx::Context, xs::Vector=copy(procs(ctx))) = # make a copy
    Context(xs; log_sink=ctx.log_sink, log_file=ctx.log_file,
                profile=ctx.profile, options=ctx.options)

const GLOBAL_CONTEXT = Ref{Context}()
function global_context()
    if !isassigned(GLOBAL_CONTEXT)
        GLOBAL_CONTEXT[] = Context()
    end
    return GLOBAL_CONTEXT[]
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

# In-Thunk Helpers

"""
    thunk_processor()

Get the current processor executing the current thunk.
"""
thunk_processor() = task_local_storage(:_dagger_processor)::Processor

"""
    in_thunk()

Returns `true` if currently in a [`Thunk`](@ref) process, else `false`.
"""
in_thunk() = haskey(task_local_storage(), :_dagger_sch_uid)

"""
    get_tls()

Gets all Dagger TLS variable as a `NamedTuple`.
"""
get_tls() = (
    sch_uid=task_local_storage(:_dagger_sch_uid),
    sch_handle=task_local_storage(:_dagger_sch_handle),
    processor=thunk_processor(),
    utilization=task_local_storage(:_dagger_utilization),
)

"""
    set_tls!(tls)

Sets all Dagger TLS variables from the `NamedTuple` `tls`.
"""
function set_tls!(tls)
    task_local_storage(:_dagger_sch_uid, tls.sch_uid)
    task_local_storage(:_dagger_sch_handle, tls.sch_handle)
    task_local_storage(:_dagger_processor, tls.processor)
    task_local_storage(:_dagger_utilization, tls.utilization)
end
