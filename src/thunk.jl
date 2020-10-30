export Thunk, delayed, delayedmap

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
                   options=nothing
                  )
        new(f,xs,id,get_result,meta,persist, cache, cache_ref, affinity, options)
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

"""
    @par f(args...) -> Thunk

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
"""
macro par(ex)
    _par(ex)
end
function _par(ex::Expr)
    if ex.head == :call
        f = ex.args[1]
        args = ex.args[2:end]
        # TODO: Support kwargs
        return :(Dagger.delayed($(esc(f)))($(_par.(args)...)))
    else
        return Expr(ex.head, _par.(ex.args)...)
    end
end
_par(ex::Symbol) = esc(ex)
_par(ex) = ex

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

