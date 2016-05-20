let counter=0
    global next_id
    next_id() = counter+=1
end

global _thunk_dict = Dict{Int, Any}()

# A thing to run
type Thunk <: AbstractPart
    f::Function
    inputs::Tuple
    id::Int
    get_result::Bool # whether the worker should send the result or only the metadata
    administrative::Bool
    persist::Bool
    function Thunk(f, inputs, id, get_res, meta,persist)
        x = new(f, inputs, id, get_res, meta,persist)
        _thunk_dict[id] = x
        x
    end
end
Thunk(f::Function, xs::Tuple;
      id::Int=next_id(),
      get_result::Bool=false,
      meta::Bool=false,
      persist::Bool=false) =
      Thunk(f,xs,id,get_result,meta,persist)

Thunk(f::Function, xs::AbstractArray;
      id::Int=next_id(),
      get_result::Bool=false,
      meta::Bool=false,
      persist::Bool=false) =
      Thunk(f,(xs...), id=id,get_result=get_result,meta=meta,persist=persist)

persist!(t::Thunk) = (t.persist=true; t)

# cut down 30kgs in microseconds with one weird trick
@generated function compose{N}(f, g, t::NTuple{N})
    if N <= 4
      ( :(()->f(g())),
        :((a)->f(g(a))),
        :((a,b)->f(g(a,b))),
        :((a,b,c)->f(g(a,b,c))),
        :((a,b,c,d)->f(g(a,b,c,d))), )[N+1]
    else
        :((xs...) -> f(g(xs...)))
    end
end

# function Thunk(f::Function, t::Tuple{Thunk})
#     g = compose(f, t[1].f, t[1].inputs)
#     @logmsg(string("FUSING ", f, "*", t[1].f))
#     Thunk(g, t[1].inputs)
# end

# this gives a ~30x speedup in hashing
Base.hash(x::Thunk, h::UInt) = hash(x.id, hash(h, 0x7ad3bac49089a05f))
Base.isequal(x::Thunk, y::Thunk) = x.id==y.id

get_sub(x::AbstractPart, d) = sub(x,d)
get_sub(x, d) = part(x[d])
function sub(thunk::Thunk, d::Domain, T=Any)
    Thunk(x->get_sub(x,d), (thunk,))
end

##### Macro sugar #####
function async_call(expr::Expr)
    if expr.head == :call
        f = expr.args[1]
        args = map(async_call, expr.args[2:end])
        :(Thunk($f, ($(args...),)))
    elseif expr.head == :(=)
        cll = async_call(expr.args[2])
        :($x = $cll)
    end
end

function async_call(expr)
    expr
end

macro par(expr)
    if expr.head in [:(=), :call]
        async_call(expr) |> esc
    elseif expr.head == :block
        Expr(:block, map(async_call, expr.args)...) |> esc
    else
        error("@par only works for function call and assignments")
    end
end


function Base.show(io::IO, p::Thunk)
    write(io, "*$(p.id)*")
end

inputs(x::Thunk) = x.inputs
inputs(x) = ()

istask(x::Thunk) = true
istask(x) = false

