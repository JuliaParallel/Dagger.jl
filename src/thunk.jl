export Thunk, delayed

const ID_COUNTER = Threads.Atomic{Int}(1)
next_id() = Threads.atomic_add!(ID_COUNTER, 1)

function filterany(f::Base.Callable, xs)
    xs_filt = Any[]
    for x in xs
        if f(x)
            push!(xs_filt, x)
        end
    end
    return xs_filt
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
- `f`: The function to be called upon execution of the `Thunk`.
- `args`: The arguments to be passed to the `Thunk`.
- `kwargs`: The properties describing unique behavior of this `Thunk`. Details
for each property are described in the next section.
- `option=value`: The same as passing `kwargs` to `delayed`.

## Public Properties
- `meta::Bool=false`: If `true`, instead of fetching cached arguments from
`Chunk`s and passing the raw arguments to `f`, instead pass the `Chunk`. Useful
for doing manual fetching or manipulation of `Chunk` references. Non-`Chunk`
arguments are still passed as-is.
- `processor::Processor=OSProc()` - The processor associated with `f`. Useful if
`f` is a callable struct that exists on a given processor and should be
transferred appropriately.
- `scope::Dagger.AbstractScope=DefaultScope()` - The scope associated with `f`.
Useful if `f` is a function or callable struct that may only be transferred to,
and executed within, the specified scope.

## Options
- `options`: A `Sch.ThunkOptions` struct providing the options for the `Thunk`.
If omitted, options can also be specified by passing key-value pairs as
`kwargs`.
"""
mutable struct Thunk
    f::Any # usually a Function, but could be any callable
    inputs::Vector{Pair{Union{Symbol,Nothing},Any}} # TODO: Use `ImmutableArray` in 1.8
    syncdeps::Set{Any}
    id::Int
    get_result::Bool # whether the worker should send the result or only the metadata
    meta::Bool
    persist::Bool # don't `free!` result after computing
    cache::Bool   # release the result giving the worker an opportunity to
                  # cache it
    cache_ref::Any
    affinity::Union{Nothing, Pair{OSProc, Int}}
    eager_ref::Union{DRef,Nothing}
    options::Any # stores scheduler-specific options
    propagates::Tuple # which options we'll propagate
    function Thunk(f, xs...;
                   syncdeps=nothing,
                   id::Int=next_id(),
                   get_result::Bool=false,
                   meta::Bool=false,
                   persist::Bool=false,
                   cache::Bool=false,
                   cache_ref=nothing,
                   affinity=nothing,
                   eager_ref=nothing,
                   processor=nothing,
                   scope=nothing,
                   options=nothing,
                   propagates=(),
                   kwargs...
                  )
        if !isa(f, Chunk) && (!isnothing(processor) || !isnothing(scope))
            f = tochunk(f,
                        something(processor, OSProc()),
                        something(scope, DefaultScope()))
        end
        xs = Base.mapany(identity, xs)
        syncdeps_set = Set{Any}(filterany(is_task_or_chunk, Base.mapany(last, xs)))
        if syncdeps !== nothing
            for dep in syncdeps
                push!(syncdeps_set, dep)
            end
        end
        @assert all(x->x isa Pair, xs)
        if options !== nothing
            @assert isempty(kwargs)
            new(f, xs, syncdeps_set, id, get_result, meta, persist, cache,
                cache_ref, affinity, eager_ref, options, propagates)
        else
            new(f, xs, syncdeps_set, id, get_result, meta, persist, cache,
                cache_ref, affinity, eager_ref, Sch.ThunkOptions(;kwargs...),
                propagates)
        end
    end
end
Serialization.serialize(io::AbstractSerializer, t::Thunk) =
    throw(ArgumentError("Cannot serialize a Thunk"))

function affinity(t::Thunk)
    if t.affinity !== nothing
        return t.affinity
    end

    if t.cache && t.cache_ref !== nothing
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

function args_kwargs_to_pairs(args, kwargs)
    args_kwargs = Pair{Union{Symbol,Nothing},Any}[]
    for arg in args
        push!(args_kwargs, nothing => arg)
    end
    for kwarg in kwargs
        push!(args_kwargs, kwarg[1] => kwarg[2])
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
function delayed(f, options::Options)
    (args...; kwargs...) -> Thunk(f, args_kwargs_to_pairs(args, kwargs)...; options.options...)
end
delayed(f; kwargs...) = delayed(f, Options(;kwargs...))

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
Base.show(io::IO, t::WeakThunk) = (print(io, "~"); Base.show(io, t.x.value))
Base.convert(::Type{WeakThunk}, t::Thunk) = WeakThunk(t)

struct ThunkFailedException{E<:Exception} <: Exception
    thunk::WeakThunk
    origin::WeakThunk
    ex::E
end
ThunkFailedException(thunk, origin, ex::E) where E =
    ThunkFailedException{E}(convert(WeakThunk, thunk), convert(WeakThunk, origin), ex)
function Base.showerror(io::IO, ex::ThunkFailedException)
    t = unwrap_weak(ex.thunk)

    # Find root-cause thunk
    last_tfex = ex
    failed_tasks = Union{Thunk,Nothing}[]
    while last_tfex.ex isa ThunkFailedException && unwrap_weak(last_tfex.ex.origin) !== nothing
        push!(failed_tasks, unwrap_weak(last_tfex.thunk))
        last_tfex = last_tfex.ex
    end
    o = unwrap_weak(last_tfex.origin)
    root_ex = last_tfex.ex

    function thunk_string(t)
        if t === nothing
            return "Thunk(?)"
        end
        Tinputs = Any[]
        for (_, input) in t.inputs
            input = unwrap_weak(input)
            if istask(input)
                push!(Tinputs, "Thunk(id=$(input.id))")
            else
                push!(Tinputs, input)
            end
        end
        t_sig = if length(Tinputs) <= 4
            "$(t.f)($(join(Tinputs, ", ")))"
        else
            "$(t.f)($(length(Tinputs)) inputs...)"
        end
        return "Thunk(id=$(t.id), $t_sig)"
    end
    t_str = thunk_string(t)
    o_str = thunk_string(o)
    t_id = t !== nothing ? t.id : '?'
    o_id = o !== nothing ? o.id : '?'
    println(io, "ThunkFailedException:")
    println(io, "  Root Exception Type: $(typeof(root_ex))")
    println(io, "  Root Exception:")
    Base.showerror(io, root_ex); println(io)
    if t !== o
        println(io, "  Root Thunk:  $o_str")
        if length(failed_tasks) <= 4
            for i in failed_tasks
                i_str = thunk_string(i)
                println(io, "  Inner Thunk: $i_str")
            end
        else
            println(io, " ...")
            println(io, "  $(length(failed_tasks)) Inner Thunks...")
            println(io, " ...")
        end
    end
    print(io, "  This Thunk:  $t_str")
end

"""
    spawn(f, args...; kwargs...) -> EagerThunk

Spawns a task with `f` as the function, `args` as the arguments, and `kwargs`
as the keyword arguments, returning an `EagerThunk`. Uses a scheduler running
in the background to execute code.
"""
function spawn(f, args...; kwargs...)
    @nospecialize f args kwargs

    # Get all options and determine which propagate beyond this task
    options = get_options()
    propagates = get(options, :propagates, ())
    propagates = Tuple(unique(Symbol[propagates..., keys(options)...]))
    if length(args) >= 1 && first(args) isa Options
        spawn_options = first(args).options
        options = merge(options, spawn_options)
        args = args[2:end]
    end

    # Wrap f in a Chunk if necessary
    processor = haskey(options, :processor) ? options.processor : nothing
    scope = haskey(options, :scope) ? options.scope : nothing
    if !isnothing(processor) || !isnothing(scope)
        f = tochunk(f,
                    something(processor, get_options(:processor, OSProc())),
                    something(scope, get_options(:scope, DefaultScope())))
    end

    # Process the args and kwargs into Pair form
    args_kwargs = args_kwargs_to_pairs(args, kwargs)

    # Get task queue, and don't let it propagate
    task_queue = get_options(:task_queue, EagerTaskQueue())
    options = NamedTuple(filter(opt->opt[1] != :task_queue, Base.pairs(options)))
    propagates = filter(prop->prop != :task_queue, propagates)
    options = merge(options, (;propagates))

    # Construct task spec and handle
    spec = EagerTaskSpec(f, args_kwargs, options)
    task = eager_spawn(spec)

    # Enqueue the task into the task queue
    enqueue!(task_queue, spec=>task)

    return task
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
    _par(ex; lazy=true, opts=opts)
end

"""
    @spawn [opts] f(args...) -> Thunk

Convenience macro like `Dagger.@par`, but eagerly executed from the moment it's
called (equivalent to `spawn`).

See the docs for `@par` for more information and usage examples.
"""
macro spawn(exs...)
    opts = exs[1:end-1]
    ex = exs[end]
    _par(ex; lazy=false, opts=opts)
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

function _par(ex::Expr; lazy=true, recur=true, opts=())
    if ex.head == :call && recur
        f = replace_broadcast(ex.args[1])
        if length(ex.args) >= 2 && Meta.isexpr(ex.args[2], :parameters)
            args = ex.args[3:end]
            kwargs = ex.args[2]
        else
            args = ex.args[2:end]
            kwargs = Expr(:parameters)
        end
        opts = esc.(opts)
        args_ex = _par.(args; lazy=lazy, recur=false)
        kwargs_ex = _par.(kwargs.args; lazy=lazy, recur=false)
        if lazy
            return :(Dagger.delayed($(esc(f)), $Options(;$(opts...)))($(args_ex...); $(kwargs_ex...)))
        else
            sync_var = esc(Base.sync_varname)
            @gensym result
            return quote
                let args = ($(args_ex...),)
                    $result = $spawn($(esc(f)), $Options(;$(opts...)), args...; $(kwargs_ex...))
                    if $(Expr(:islocal, sync_var))
                        put!($sync_var, schedule(Task(()->wait($result))))
                    end
                    $result
                end
            end
        end
    else
        return Expr(ex.head, _par.(ex.args, lazy=lazy, recur=recur, opts=opts)...)
    end
end
_par(ex::Symbol; kwargs...) = esc(ex)
_par(ex; kwargs...) = ex

persist!(t::Thunk) = (t.persist=true; t)
cache_result!(t::Thunk) = (t.cache=true; t)

# @generated function compose{N}(f, g, t::NTuple{N})
#     if N <= 4
#       ( :(()->f(g())),
#         :((a)->f(g(a))),
#         :((a,b)->f(g(a,b))),
#         :((a,b,c)->f(g(a,b,c))),
#         :((a,b,c,d)->f(g(a,b,c,d))), )[N+1]
#     else
#         :((xs...) -> f(g(xs...)))
#     end
# end

# function Thunk(f::Function, t::Tuple{Thunk})
#     g = compose(f, t[1].f, t[1].inputs)
#     Thunk(g, t[1].inputs)
# end

# this gives a ~30x speedup in hashing
Base.hash(x::Thunk, h::UInt) = hash(x.id, hash(h, 0x7ad3bac49089a05f % UInt))
Base.isequal(x::Thunk, y::Thunk) = x.id==y.id

function Base.show(io::IO, z::Thunk)
    lvl = get(io, :lazy_level, 2)
    f = if z.f isa Chunk
        Tf = z.f.chunktype
        if isdefined(Tf, :instance)
            Tf.instance
        else
            "instance of $Tf"
        end
    else
        z.f
    end
    print(io, "Thunk[$(z.id)]($f, ")
    if lvl > 0
        inputs = Any[]
        for (pos, input) in z.inputs
            if pos === nothing
                push!(inputs, input)
            else
                push!(inputs, pos => input)
            end
        end
        show(IOContext(io, :lazy_level => lvl-1), inputs)
    else
        print(io, "...")
    end
    print(io, ")")
end

Base.summary(z::Thunk) = repr(z)

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
task_id(x::Thunk) = x.id
istask(x) = false
