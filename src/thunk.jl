export Thunk, delayed, delayedmap

# TODO: Make this thread-safe
let counter=0
    global next_id
    next_id() = (counter >= (1 << 30)) ? (counter = 1) : (counter += 1)
end

# A thing to run
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
    affinity::Union{Nothing, Vector{Pair{OSProc, Int}}}
    options::Any # stores scheduler-specific options
    function Thunk(f, xs...;
                   id::Int=next_id(),
                   get_result::Bool=false,
                   meta::Bool=false,
                   persist::Bool=false,
                   cache::Bool=false,
                   cache_ref=nothing,
                   affinity=nothing,
                   options=nothing,
                   kwargs...
                  )
        if options !== nothing
            @assert isempty(kwargs)
            new(f, xs, id, get_result, meta, persist, cache, cache_ref,
                affinity, options)
        else
            new(f, xs, id, get_result, meta, persist, cache, cache_ref,
                affinity, Sch.ThunkOptions(;kwargs...))
        end
    end
end

function affinity(t::Thunk)
    if t.affinity !== nothing
        @logmsg("$t has (cached) affinity: $(get(t.affinity))")
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
                    for a in affinity(inp)
                        proc, sz = a
                        aff[proc] = get(aff, proc, 0) + sz
                    end
                end
           #end
        end
        aff_vec = collect(aff)
    end
    @logmsg("$t has affinity: $aff_vec")
    aff_vec
   #if length(aff) > 1
   #    return sort!(aff_vec, by=last,rev=true)
   #else
   #    return aff_vec
   #end
end

function delayed(f; kwargs...)
    (args...) -> Thunk(f, args...; kwargs...)
end

delayedmap(f, xs...) = map(delayed(f), xs...)

struct ThunkFuture
    future::Future
end
ThunkFuture(x::Integer) = ThunkFuture(Future(x))
ThunkFuture() = ThunkFuture(Future())
Base.isready(t::ThunkFuture) = isready(t.future)
Base.wait(t::ThunkFuture) = wait(t.future)
function Base.fetch(t::ThunkFuture; proc=OSProc())
    error, value = move(proc, fetch(t.future))
    if error
        throw(value)
    end
    value
end
Base.put!(t::ThunkFuture, x; error=false) = put!(t.future, (error, x))

struct ThunkFailedException{E<:Exception} <: Exception
    thunk::Thunk
    origin::Thunk
    ex::E
end
function Base.showerror(io::IO, ex::ThunkFailedException)
    println(io, "$(ex.thunk) (id $(ex.thunk.id)) failure",
                ex.thunk !== ex.origin ? " due to a failure in $(ex.origin)" : "",
                ":")
    Base.showerror(io, ex.ex)
end

struct EagerThunk
    future::ThunkFuture
    uid::Int
end
Base.isready(t::EagerThunk) = isready(t.future)
Base.wait(t::EagerThunk) = wait(t.future)
Base.fetch(t::EagerThunk) = move(OSProc(), fetch(t.future))
function Base.show(io::IO, t::EagerThunk)
    print(io, "EagerThunk ($(isready(t) ? "finished" : "running"))")
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
called. Uses a scheduler running in the background to execute code.
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
                Dagger.Sch.init_eager()
                future = $ThunkFuture()
                uid = $next_id()
                put!(Dagger.Sch.EAGER_THUNK_CHAN, (future, uid, $(esc(f)), ($(_par.(args; lazy=lazy, recur=false)...),), ($(opts...),)))
                EagerThunk(future, uid)
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
#     @logmsg(string("FUSING ", f, "*", t[1].f))
#     Thunk(g, t[1].inputs)
# end

# this gives a ~30x speedup in hashing
Base.hash(x::Thunk, h::UInt) = hash(x.id, hash(h, 0x7ad3bac49089a05f % UInt))
Base.isequal(x::Thunk, y::Thunk) = x.id==y.id

function Base.show(io::IO, z::Thunk)
    lvl = get(io, :lazy_level, 1)
    print(io, "Thunk($(z.f), ")
    if lvl < 2
        show(IOContext(io, :lazy_level => lvl+1), z.inputs)
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
