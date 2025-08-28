export Thunk, delayed

const ID_COUNTER = Threads.Atomic{Int}(1)
next_id() = Threads.atomic_add!(ID_COUNTER, 1)

const EMPTY_ARGS = Argument[]
const EMPTY_SYNCDEPS = Set{Any}()
Base.@kwdef mutable struct ThunkSpec
    fargs::Vector{Argument} = EMPTY_ARGS
    world::UInt64 = UInt64(0)
    id::Int = 0
    cache_ref::Any = nothing
    affinity::Union{Pair{OSProc,Int}, Nothing} = nothing
    options::Union{Options, Nothing} = nothing
end
function unset!(spec::ThunkSpec, _)
    spec.fargs = EMPTY_ARGS
    spec.world = UInt64(0)
    spec.id = 0
    spec.cache_ref = nothing
    spec.affinity = nothing
    compute_scope = DefaultScope()
    result_scope = AnyScope()
    spec.options = nothing
end

"""
    Thunk

Wraps a callable object to be run with Dagger. A `Thunk` is typically
created through a call to `delayed` or its macro equivalent `@par`.

## Constructors
```julia
delayed(f; kwargs...)(args...)
@par [option=value]... f(args...)
```

## Examples
```julia
julia> t = delayed(sin)(π)  # creates a Thunk to be computed later
Thunk(sin, (π,))

julia> collect(t)  # computes the result and returns it to the current process
1.2246467991473532e-16
```

## Arguments
- `fargs`: The function and arguments to be called upon execution of the `Thunk`.
- `kwargs`: The properties describing unique behavior of this `Thunk`. Details
for each property are described in the next section.
- `option=value`: The same as passing `kwargs` to `delayed`.

## Options
- `options`: An `Options` struct providing the options for the `Thunk`.
If omitted, options can also be specified by passing key-value pairs as
`kwargs`.
"""
mutable struct Thunk
    inputs::Vector{Argument} # TODO: Use `ImmutableArray` in 1.8
    world::UInt64
    id::Int
    cache_ref::Any
    affinity::Union{Pair{OSProc,Int}, Nothing}
    options::Union{Options, Nothing} # stores task options
    eager_accessible::Bool
    sch_accessible::Bool
    finished::Bool
    function Thunk(spec::ThunkSpec)
        return new(spec.fargs, spec.world, spec.id,
                   spec.cache_ref, spec.affinity,
                   spec.options,
                   true, true, false)
    end
end
function Thunk(f, xs...;
               world::UInt64=Base.get_world_counter(),
               syncdeps=nothing,
               id::Int=next_id(),
               cache_ref=nothing,
               affinity=nothing,
               options=nothing,
               propagates=(),
               kwargs...
              )

    spec = ThunkSpec()
    if !(f isa Argument)
        f = Argument(ArgPosition(true, 0, :NULL), f)
    end
    spec.fargs = Vector{Argument}(undef, length(xs)+1)
    spec.fargs[1] = f
    for idx in 1:length(xs)
        x = xs[idx]
        if x isa Argument
            spec.fargs[idx+1] = x
        else
            @assert x isa Pair "Invalid Thunk argument: $x"
            spec.fargs[idx+1] = Argument(something(x.first, idx), x.second)
        end
    end
    spec.world = world
    if options === nothing
        options = Options()
    end
    spec.options = options::Options
    if options.syncdeps === nothing
        options.syncdeps = Set{Any}()
    end
    syncdeps_set = options.syncdeps
    for idx in 2:length(spec.fargs)
        x = value(spec.fargs[idx])
        if is_task_or_chunk(x)
            push!(syncdeps_set, x)
        end
    end
    if syncdeps !== nothing
        for dep in syncdeps
            push!(syncdeps_set, dep)
        end
    end
    spec.id = id
    if kwargs !== nothing
        options_merge!(options, (;kwargs...))
    end
    if haskey(kwargs, :cache)
        @warn "The cache argument is deprecated, as it is now always true" maxlog=1
    end
    spec.cache_ref = cache_ref
    spec.affinity = affinity
    return Thunk(spec)
end
Serialization.serialize(io::AbstractSerializer, t::Thunk) =
    throw(ArgumentError("Cannot serialize a Thunk"))
function Base.getproperty(thunk::Thunk, field::Symbol)
    if field == :f
        return unwrap_weak_checked(value(first(thunk.inputs)))
    else
        return getfield(thunk, field)
    end
end

function affinity(t::Thunk)
    if t.affinity !== nothing
        return t.affinity
    end

    if t.cache_ref !== nothing
        aff_vec = affinity(t.cache_ref)
    else
        aff = Dict{OSProc,Int}()
        for (_, inp) in t.inputs
           #if haskey(state.cache, inp)
           #    as = affinity(state.cache[inp])
           #    for a in as
           #        proc, sz = a
           #        aff[proc] = get(aff, proc, 0) + sz
           #    end
           #else
                if isa(inp, Union{Chunk, Thunk})
                    # TODO if inp is a FileRef, affinity[1] will always be OSProc(1)
                    proc, sz = affinity(inp)
                    aff[proc] = get(aff, proc, 0) + sz
                end
           #end
        end
        aff_vec = collect(aff)
    end
    aff_vec
   #if length(aff) > 1
   #    return sort!(aff_vec, by=last,rev=true)
   #else
   #    return aff_vec
   #end
end

is_task_or_chunk(x) = istask(x)

function args_kwargs_to_arguments(f, args, kwargs)
    @nospecialize f args kwargs
    args_kwargs = Argument[]
    push!(args_kwargs, Argument(ArgPosition(true, 0, :NULL), f))
    for idx in 1:length(args)
        arg = args[idx]
        push!(args_kwargs, Argument(idx, arg))
    end
    for (kw, value) in kwargs
        push!(args_kwargs, Argument(kw, value))
    end
    return args_kwargs
end
function args_kwargs_to_arguments(f, args)
    @nospecialize f args
    args_kwargs = Argument[]
    push!(args_kwargs, Argument(ArgPosition(true, 0, :NULL), f))
    pos_ctr = 1
    for idx in 1:length(args)
        pos, arg = args[idx]::Pair
        if pos === nothing
            push!(args_kwargs, Argument(pos_ctr, arg))
            pos_ctr += 1
        else
            push!(args_kwargs, Argument(pos, arg))
        end
    end
    return args_kwargs
end

"""
    delayed(f, options=Options())(args...; kwargs...) -> Thunk
    delayed(f; options...)(args...; kwargs...) -> Thunk

Creates a [`Thunk`](@ref) object which can be executed later, which will call
`f` with `args` and `kwargs`. `options` controls various properties of the
resulting `Thunk`.
"""
function _delayed(f, options::Options)
    (args...; kwargs...) -> Thunk(args_kwargs_to_arguments(f, args, kwargs)...; options)
end
function delayed(f, options::Options)
    @warn "`delayed` is deprecated. Use `Dagger.@spawn` or `Dagger.spawn` instead." maxlog=1
    return _delayed(f, options)
end
function delayed(f; options=nothing, kwargs...)
    if options !== nothing
        options = options::Options
    else
        options = Options()
    end
    options_merge!(options, kwargs; override=true)
    return delayed(f, options)
end

"A weak reference to a `Thunk`."
struct WeakThunk
    x::WeakRef
    WeakThunk(t::Thunk) = new(WeakRef(t))
end
istask(::WeakThunk) = true
task_id(t::WeakThunk) = task_id(unwrap_weak_checked(t))
unwrap_weak(t::WeakThunk) = t.x.value
unwrap_weak(t) = t
function unwrap_weak_checked(t::WeakThunk)
    t = unwrap_weak(t)
    @assert t !== nothing
    t
end
unwrap_weak_checked(t) = t
wrap_weak(t::Thunk) = WeakThunk(t)
wrap_weak(t::WeakThunk) = t
wrap_weak(t) = t
isweak(t::WeakThunk) = true
isweak(t::Thunk) = false
isweak(t) = true
Base.show(io::IO, t::WeakThunk) = (print(io, "~"); Base.show(io, t.x.value))
Base.convert(::Type{WeakThunk}, t::Thunk) = WeakThunk(t)
chunktype(t::WeakThunk) = chunktype(unwrap_weak_checked(t))

"A summary of the data contained in a Thunk, which can be safely serialized."
struct ThunkSummary
    id::Int
    inputs::Vector{Argument}
end
inputs(t::ThunkSummary) = t.inputs
Base.show(io::IO, t::ThunkSummary) = show_thunk(io, t)
function Base.convert(::Type{ThunkSummary}, t::Thunk)
    args = map(copy, t.inputs)
    for arg in args
        if istask(value(arg))
            arg.value = convert(ThunkSummary, value(arg))
        end
    end
    return ThunkSummary(t.id, args)
end
function Base.convert(::Type{ThunkSummary}, t::WeakThunk)
    t = unwrap_weak(t)
    if t !== nothing
        t = convert(ThunkSummary, t)
    end
    return t
end

struct DTaskFailedException{E<:Exception} <: Exception
    thunk::ThunkSummary
    origin::ThunkSummary
    ex::E
end
DTaskFailedException(thunk, origin, ex::E) where E =
    DTaskFailedException{E}(convert(ThunkSummary, thunk),
                            convert(ThunkSummary, origin),
                            ex)
@deprecate ThunkFailedException DTaskFailedException
function Base.showerror(io::IO, ex::DTaskFailedException)
    t = ex.thunk

    # Find root-cause thunk
    last_tfex = ex
    failed_tasks = Union{ThunkSummary,Nothing}[]
    while last_tfex.ex isa DTaskFailedException
        push!(failed_tasks, last_tfex.thunk)
        last_tfex = last_tfex.ex
    end
    o = last_tfex.origin
    root_ex = last_tfex.ex

    function thunk_string(t)
        Tinputs = Any[]
        for input in @view t.inputs[2:end]
            x = value(input)
            if istask(x)
                push!(Tinputs, "DTask(id=$(x.id))")
            else
                push!(Tinputs, x)
            end
        end
        f = value(t.inputs[1])
        t_sig = if length(Tinputs) <= 4
            "$(f)($(join(Tinputs, ", ")))"
        else
            "$(f)($(length(Tinputs)) inputs...)"
        end
        return "DTask(id=$(t.id), $t_sig)"
    end
    t_str = thunk_string(t)
    o_str = thunk_string(o)
    println(io, "DTaskFailedException:")
    println(io, "  Root Exception Type: $(typeof(Sch.unwrap_nested_exception(root_ex)))")
    println(io, "  Root Exception:")
    Base.showerror(io, root_ex); println(io)
    if t.id !== o.id
        println(io, "  Root Task:  $o_str")
        if length(failed_tasks) <= 4
            for i in failed_tasks
                i_str = thunk_string(i)
                println(io, "  Inner Task: $i_str")
            end
        else
            println(io, " ...")
            println(io, "  $(length(failed_tasks)) Inner Tasks...")
            println(io, " ...")
        end
    end
    print(io, "  This Task:  $t_str")
end

"""
    @par [opts] f(args...; kwargs...) -> Thunk

Convenience macro to call `Dagger.delayed` on `f` with arguments `args` and
keyword arguments `kwargs`. May also be called with a series of assignments
like so:

```julia
x = @par begin
    a = f(1,2)
    b = g(a,3)
    h(a,b)
end
```

`x` will hold the Thunk representing `h(a,b)`; additionally, `a` and `b`
will be defined in the same local scope and will be equally accessible
for later calls.

Options to the `Thunk` can be set as `opts` with namedtuple syntax, e.g.
`single=1`. Multiple options may be provided, and will be applied to all
generated thunks.
"""
macro par(exs...)
    opts = exs[1:end-1]
    ex = exs[end]
    return esc(_par(__module__, ex; lazy=true, opts=opts))
end

"""
    Dagger.@spawn [option=value]... f(args...; kwargs...) -> DTask

Spawns a Dagger `DTask` that will call `f(args...; kwargs...)`. This `DTask` is like a Julia `Task`, and has many similarities:
- The `DTask` can be `wait`'d on and `fetch`'d from to see its final result
- By default, the `DTask` will be automatically run on the first available compute resource
- If all dependencies are satisfied, the `DTask` will be run as soon as possible
- The `DTask` may be run in parallel with other `DTask`s, and the scheduler will automatically manage dependencies
- If a `DTask` throws an exception, it will be propagated to any calls to `fetch`, but not to calls to `wait`

However, the `DTask` also has many key differences from a `Task`:
- The `DTask` may run on any thread of any Julia process, and even on a remote machine, in your cluster (see `Distributed.addprocs`)
- The `DTask` might automatically utilize GPUs or other accelerators, if available
- If arguments to a `DTask` are also `DTask`s, then the scheduler will execute those arguments' `DTask`s first, before running the "downstream" task
- If an argument to a `DTask` `t2` is a `DTask` `t1`, then the *result* of `t1` (gotten via `fetch(t1)`) will be passed to `t2` (no need for `t2` to call `fetch`!)
- `DTask`s are generally expected to be defined "functionally", meaning that they should not mutate global state, mutate their arguments, or have side effects
- `DTask`s are function call-focused, meaning that `Dagger.@spawn` expects a single function call, and not a block of code
- All `DTask` arguments are expected to be safe to serialize and send to other Julia processes; if not, use the `scope` option or `Dagger.@mutable` to control execution location

Options to the `DTask` can be set before the call to `f` with key-value syntax, e.g.
`Dagger.@spawn myopt=2 do_something(1, 3.0)`, which would set the option
`myopt` to `2` for this task. Multiple options may be provided, which are
specified like `Dagger.@spawn myopt=2 otheropt=4 do_something(1, 3.0)`.

These options control a variety of properties of the resulting `DTask`:
- `scope`: The execution "scope" of the task, which determines where the task will run. By default, the task will run on the first available compute resource. If you have multiple compute resources, you can specify a scope to run the task on a specific resource. For example, `Dagger.@spawn scope=Dagger.scope(worker=2) do_something(1, 3.0)` would run `do_something(1, 3.0)` on worker 2.
- `meta`: If `true`, instead of the scheduler automatically fetching values from other tasks, the raw `Chunk` objects will be passed to `f`. Useful for doing manual fetching or manipulation of `Chunk` references. Non-`Chunk` arguments are still passed as-is.

Other options exist; see `Dagger.Options` for the full list.

This macro is a semi-thin wrapper around `Dagger.spawn` - it creates a call to
`Dagger.spawn` on `f` with arguments `args` and keyword arguments `kwargs`, and
also passes along any options in an `Options` struct. For example,
`Dagger.@spawn myopt=2 do_something(1, 3.0)` would essentially become
`Dagger.spawn(do_something, Dagger.Options(;myopt=2), 1, 3.0)`.
"""
macro spawn(exs...)
    opts = exs[1:end-1]
    ex = exs[end]
    return esc(_par(__module__, ex; lazy=false, opts=opts))
end

struct ExpandedBroadcast{F} end
(eb::ExpandedBroadcast{F})(args...) where F =
    Base.materialize(Base.broadcasted(F, args...))
replace_broadcast(ex) = ex
function replace_broadcast(fn::Symbol)
    if startswith(string(fn), '.')
        return :($ExpandedBroadcast{$(Symbol(string(fn)[2:end]))}())
    end
    return fn
end

to_namedtuple(;kwargs...) = (;kwargs...)

function _par(mod, ex::Expr; lazy=true, recur=true, opts=())
    f = nothing
    bf = nothing
    body = nothing
    arg1 = nothing
    arg2 = nothing
    value = nothing
    if recur && @capture(ex, f_(allargs__)) ||
                @capture(ex, f_(allargs__) do cargs_ body_ end) ||
                @capture(ex, allargs__->body_) ||
                @capture(ex, arg1_[allargs__]) ||
                @capture(ex, arg1_[allargs__] = value_) ||
                @capture(ex, arg1_.arg2_) ||
                @capture(ex, (;allargs__)) ||
                @capture(ex, bf_.(allargs__))
        if bf !== nothing
            f = ExpandedBroadcast{mod.eval(bf)}()
        end
        f = replace_broadcast(f)
        if arg1 !== nothing
            if arg2 !== nothing
                # Getproperty (A.B)
                f = Base.getproperty
                allargs = Any[arg1, QuoteNode(arg2)]
            elseif value !== nothing
                # setindex! (A[2,3] = 4)
                f = _setindex!_return_value
                pushfirst!(allargs, value)
                pushfirst!(allargs, arg1)
            else
                # getindex (A[2,3])
                f = Base.getindex
                pushfirst!(allargs, arg1)
            end
        end
        if f === nothing && body === nothing
            # NamedTuple ((;a=1, b=2))
            f = to_namedtuple
        end
        args = filter(arg->!Meta.isexpr(arg, :parameters), allargs)
        kwargs = filter(arg->Meta.isexpr(arg, :parameters), allargs)
        if !isempty(kwargs)
            kwargs = only(kwargs).args
        end
        if body !== nothing
            if f !== nothing
                f = quote
                    ($(args...); $(kwargs...))->$f($(args...); $(kwargs...)) do $cargs
                        $body
                    end
                end
            else
                f = quote
                    ($(args...); $(kwargs...))->begin
                        $body
                    end
                end
            end
        end
        if lazy
            return :(Dagger.delayed($f, $Options(;$(opts...)))($(args...); $(kwargs...)))
        else
            sync_var = Base.sync_varname
            @gensym result
            return quote
                let
                    $result = $spawn($f, $Options(;$(opts...)), $(args...); $(kwargs...))
                    if $(Expr(:islocal, sync_var))
                        put!($sync_var, schedule(Task(()->fetch($result; raw=true))))
                    end
                    $result
                end
            end
        end
    elseif lazy
        # Recurse into the expression
        return Expr(ex.head, _par_inner.(Ref(mod), ex.args, lazy=lazy, recur=recur, opts=opts)...)
    else
        throw(ArgumentError("Invalid Dagger task expression: $ex"))
    end
end
_par(mod, ex; kwargs...) = throw(ArgumentError("Invalid Dagger task expression: $ex"))

_par_inner(mod, ex; kwargs...) = ex
_par_inner(mod, ex::Expr; kwargs...) = _par(mod, ex; kwargs...)

function _setindex!_return_value(A, value, idxs...)
    setindex!(A, value, idxs...)
    return value
end

"""
    Dagger.spawn(f, args...; kwargs...) -> DTask

Spawns a `DTask` that will call `f(args...; kwargs...)`. Also supports passing a
`Dagger.Options` struct as the first argument to set task options. See
`Dagger.@spawn` for more details on `DTask`s.
"""
function spawn(f, args...; kwargs...)
    @nospecialize f args kwargs

    # Get all scoped options and determine which propagate beyond this task
    scoped_options = get_options()::NamedTuple
    if haskey(scoped_options, :propagates)
        if scoped_options.propagates isa Tuple
            propagates = Symbol[scoped_options.propagates...]
        else
            propagates = scoped_options.propagates::Vector{Symbol}
        end
    else
        propagates = Symbol[]
    end
    append!(propagates, keys(scoped_options)::NTuple{N,Symbol} where N)

    # Merge all passed options
    if length(args) >= 1 && first(args) isa Options
        # N.B. Make a defensive copy in case user aliases Options struct
        task_options = copy(first(args)::Options)
        args = args[2:end]
    else
        task_options = Options()
    end
    # N.B. Merges into task_options
    options_merge!(task_options, scoped_options; override=false)

    # Process the args and kwargs into Pair form
    args_kwargs = args_kwargs_to_arguments(f, args, kwargs)

    # Get task queue, and don't let it propagate
    task_queue = get(scoped_options, :task_queue, DefaultTaskQueue())::AbstractTaskQueue
    filter!(prop -> prop != :task_queue, propagates)
    if task_options.propagates !== nothing
        append!(task_options.propagates, propagates)
    else
        task_options.propagates = propagates
    end
    unique!(task_options.propagates)

    # Construct task spec and handle
    spec = DTaskSpec(args_kwargs, Base.get_world_counter(), task_options)
    task = eager_spawn(spec)

    # Enqueue the task into the task queue
    enqueue!(task_queue, spec=>task)

    return task
end

# this gives a ~30x speedup in hashing
Base.hash(x::Thunk, h::UInt) = hash(x.id, hash(h, 0x7ad3bac49089a05f % UInt))
Base.isequal(x::Thunk, y::Thunk) = x.id==y.id

function show_thunk(io::IO, t)
    lvl = get(io, :lazy_level, 0)
    f = value(first(t.inputs))
    f = if f isa Chunk
        Tf = f.chunktype
        if isdefined(Tf, :instance)
            Tf.instance
        else
            "instance of $Tf"
        end
    else
        f
    end
    print(io, "Thunk[$(t.id)]($f, ")
    if lvl > 0
        t_inputs = Any[]
        for arg in inputs(t)[2:end]
            input = value(arg)
            if ispositional(arg)
                push!(t_inputs, input)
            else
                push!(t_inputs, pos_kw(arg) => input)
            end
        end
        show(IOContext(io, :lazy_level => lvl-1), t_inputs)
    else
        print(io, "...")
    end
    print(io, ")")
end
function Base.show(io::IO, t::Thunk)
    lazy_level = parse(Int, get(ENV, "JULIA_DAGGER_SHOW_THUNK_VERBOSITY", "0"))
    if lazy_level == 0
        show_thunk(io, t)
    else
        show_thunk(IOContext(io, :lazy_level => lazy_level), t)
    end
end
Base.summary(t::Thunk) = repr(t)

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
task_id(x::Thunk) = x.id
istask(x) = false
