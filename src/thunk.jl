let counter=0
    global next_id
    next_id() = counter+=1
end

global _thunk_dict = Dict()

# A thing to run
immutable Thunk <: AbstractPart
    f::Function
    inputs::Tuple
    id::Int
    administrative::Bool
    function Thunk(f, inputs, id, meta)
        x = new(f, inputs, id, meta)
        _thunk_dict[id] = x
        x
    end
end
Thunk(f::Function, xs::Tuple; id::Int=next_id(),
    meta::Bool=false) = Thunk(f,xs,id,meta)
Thunk(f::Function, xs::AbstractArray; id::Int=next_id(),
    meta::Bool=false) = Thunk(f,(xs...),id,meta)

function sub(thunk::Thunk, d::Domain, T=Any)
    Thunk(data -> Sub(parttype(data), alignfirst(d), d, part(data)),
        (thunk,))
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

