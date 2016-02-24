
# a task
immutable Thunk <: AbstractPart
    id::Symbol
    f::Function
    inputs::Tuple
end
Thunk(id::Symbol, f::Function, xs...) = Thunk(id, f,xs)
Thunk(id::AbstractString, f::Function, xs...) = Thunk(symbol(id), f,xs)
Thunk(f::Function, xs...) = Thunk(f,xs)
Thunk(f::Function, xs::Tuple) = Thunk(next_id(), f,xs)

function sub(thunk::Thunk, d::Domain, T=Any)
    Thunk(data -> Sub(parttype(data), alignfirst(d), d, part(data)), thunk)
end

