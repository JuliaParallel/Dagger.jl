export Thunk, delayed, delayedmap

let counter=0
    global next_id
    const MAX_ID = (1 << 30)
    next_id() = (counter >= MAX_ID) ? (counter = 1) : (counter += 1)
end

global _thunk_dict = Dict{Int, Any}()

# A thing to run
mutable struct Thunk
    f::Function
    inputs::Tuple
    id::Int
    get_result::Bool # whether the worker should send the result or only the metadata
    meta::Bool
    persist::Bool # don't `free!` result after computing
    cache::Bool   # release the result giving the worker an opportunity to
                  # cache it
    cache_ref::Nullable
    affinity::Nullable{Vector{Pair{Processor, Int}}}
    function Thunk(f, xs...;
                   id::Int=next_id(),
                   get_result::Bool=false,
                   meta::Bool=false,
                   persist::Bool=false,
                   cache::Bool=false,
                   cache_ref::Nullable{Any}=Nullable{Any}(),
                   affinity=Nullable(),
                  )
        thunk = new(f,xs,id,get_result,meta,persist, cache, cache_ref, affinity)
        _thunk_dict[id] = thunk
        thunk
    end
end

function affinity(t::Thunk)
    if !isnull(t.affinity)
        @logmsg("$t has (cached) affinity: $(get(t.affinity))")
        return get(t.affinity)
    end

    if t.cache && !isnull(t.cache_ref)
        aff_vec = affinity(get(t.cache_ref))
    else
        aff = Dict{Processor,Int}()
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
Base.hash(x::Thunk, h::UInt) = hash(x.id, hash(h, 0x7ad3bac49089a05f))
Base.isequal(x::Thunk, y::Thunk) = x.id==y.id

function Base.show(io::IO, p::Thunk)
    write(io, "*$(p.id)*")
end

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
istask(x) = false

