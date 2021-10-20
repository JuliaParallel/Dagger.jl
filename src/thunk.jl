export Thunk, delayed, delayedmap

const ID_COUNTER = Threads.Atomic{Int}(1)
next_id() = Threads.atomic_add!(ID_COUNTER, 1)

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
- `scope::Dagger.AbstractScope=AnyScope()` - The scope associated with `f`.
Useful if `f` is a function or callable struct that may only be transferred to,
and executed within, the specified scope.

## Options
- `options`: A `Sch.ThunkOptions` struct providing the options for the `Thunk`.
If omitted, options can also be specified by passing key-value pairs as
`kwargs`.
"""
mutable struct Thunk
    f::Any # usually a Function, but could be any callable
    inputs::Tuple
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
    function Thunk(f, xs...;
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
                   kwargs...
                  )
        if !isnothing(processor) || !isnothing(scope)
            f = tochunk(f,
                        something(processor, OSProc()),
                        something(scope, AnyScope()))
        end
        if options !== nothing
            @assert isempty(kwargs)
            new(f, xs, id, get_result, meta, persist, cache, cache_ref,
                affinity, eager_ref, options)
        else
            new(f, xs, id, get_result, meta, persist, cache, cache_ref,
                affinity, eager_ref, Sch.ThunkOptions(;kwargs...))
        end
    end
end

function affinity(t::Thunk)
    if t.affinity !== nothing
        return t.affinity
    end

    if t.cache && t.cache_ref !== nothing
        aff_vec = affinity(t.cache_ref)
    else
        aff = Dict{OSProc,Int}()
        for inp in inputs(t)
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

"""
    delayed(f; kwargs...)(args...)

Creates a [`Thunk`](@ref) object which can be executed later, which will call
`f` with `args`. `kwargs` controls various properties of the resulting `Thunk`.
"""
function delayed(f; kwargs...)
    (args...) -> Thunk(f, args...; kwargs...)
end

delayedmap(f, xs...) = map(delayed(f), xs...)

"A future holding the result of a `Thunk`."
struct ThunkFuture
    future::Future
end
ThunkFuture(x::Integer) = ThunkFuture(Future(x))
ThunkFuture() = ThunkFuture(Future())
Base.isready(t::ThunkFuture) = isready(t.future)
Base.wait(t::ThunkFuture) = Dagger.Sch.thunk_yield() do
    wait(t.future)
end
function Base.fetch(t::ThunkFuture; proc=OSProc())
    error, value = Dagger.Sch.thunk_yield() do
        move(proc, fetch(t.future))
    end
    if error
        throw(value)
    end
    value
end
Base.put!(t::ThunkFuture, x; error=false) = put!(t.future, (error, x))

"A weak reference to a `Thunk`."
struct WeakThunk
    x::WeakRef
    WeakThunk(t::Thunk) = new(WeakRef(t))
end
istask(::WeakThunk) = true
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
    o = unwrap_weak(ex.origin)
    t_str = t !== nothing ? "$t" : "?"
    o_str = o !== nothing ? "$o" : "?"
    t_id = t !== nothing ? t.id : '?'
    o_id = o !== nothing ? o.id : '?'
    println(io, "ThunkFailedException ($t failure",
                (o !== nothing && t != o) ? " due to a failure in $o)" : ")",
                ":")
    Base.showerror(io, ex.ex)
end

"""
    EagerThunk

Returned from `spawn`/`@spawn` calls. Represents a task that is in the
scheduler, potentially ready to execute, executing, or finished executing. May
be `fetch`'d or `wait`'d on at any time.
"""
mutable struct EagerThunk
    uid::UInt
    future::ThunkFuture
    finalizer_ref::DRef
    thunk_ref::DRef
end
Base.isready(t::EagerThunk) = isready(t.future)
Base.wait(t::EagerThunk) = wait(t.future)
Base.fetch(t::EagerThunk) = move(OSProc(), fetch(t.future))
function Base.show(io::IO, t::EagerThunk)
    print(io, "EagerThunk ($(isready(t) ? "finished" : "running"))")
end

"When finalized, cleans-up the associated `EagerThunk`."
mutable struct EagerThunkFinalizer
    uid::UInt
    function EagerThunkFinalizer(uid)
        x = new(uid)
        finalizer(Sch.eager_cleanup, x)
        x
    end
end

const EAGER_ID_COUNTER = Threads.Atomic{UInt64}(1)
eager_next_id() = Threads.atomic_add!(EAGER_ID_COUNTER, one(UInt64))

function _spawn(f, args...; kwargs...)
    Dagger.Sch.init_eager()
    uid = eager_next_id()
    future = ThunkFuture()
    finalizer_ref = poolset(EagerThunkFinalizer(uid))
    added_future = Future()
    put!(Dagger.Sch.EAGER_THUNK_CHAN, (added_future, future, uid, finalizer_ref, f, (args...,), (kwargs...,)))
    thunk_ref = fetch(added_future)
    return (uid, future, finalizer_ref, thunk_ref)
end
"""
    spawn(f, args...; kwargs...) -> EagerThunk

Spawns a task with `f` as the function and `args` as the arguments, returning
an `EagerThunk`. Uses a scheduler running in the background to execute code.

Note that `kwargs` are passed to the `Thunk` constructor, and are documented in
its docstring.
"""
function spawn(f, args...; processor=nothing, scope=nothing, kwargs...)
    if !isnothing(processor) || !isnothing(scope)
        f = tochunk(f,
                    something(processor, OSProc()),
                    something(scope, AnyScope()))
    end
    uid, future, finalizer_ref, thunk_ref = if myid() == 1
        _spawn(f, args...; kwargs...)
    else
        remotecall_fetch(_spawn, 1, f, args...; kwargs...)
    end
    return EagerThunk(uid, future, finalizer_ref, thunk_ref)
end

"""
    @par [opts] f(args...) -> Thunk

Convenience macro to call `Dagger.delayed` on `f` with arguments `args`.
May also be called with a series of assignments like so:

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

function _par(ex::Expr; lazy=true, recur=true, opts=())
    if ex.head == :call && recur
        f = ex.args[1]
        args = ex.args[2:end]
        opts = esc.(opts)
        if lazy
            return :(Dagger.delayed($(esc(f)); $(opts...))($(_par.(args; lazy=lazy, recur=false)...)))
        else
            return quote
                args = ($(_par.(args; lazy=lazy, recur=false)...),)
                $spawn($(esc(f)), args...; $(opts...))
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
        show(IOContext(io, :lazy_level => lvl-1), z.inputs)
    else
        print(io, "...")
    end
    print(io, ")")
end

Base.summary(z::Thunk) = repr(z)

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
istask(x) = false
