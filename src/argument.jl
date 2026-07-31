mutable struct ArgPosition
    positional::Bool
    idx::Int
    kw::Symbol
end
ArgPosition() = ArgPosition(true, 0, :NULL)
ArgPosition(pos::ArgPosition) = ArgPosition(pos.positional, pos.idx, pos.kw)
ispositional(pos::ArgPosition) = pos.positional
iskw(pos::ArgPosition) = !pos.positional
raw_position(pos::ArgPosition) = ispositional(pos) ? pos.idx : pos.kw
function pos_idx(pos::ArgPosition)
    @assert pos.positional
    @assert pos.idx > 0
    @assert pos.kw == :NULL
    return pos.idx
end
function pos_kw(pos::ArgPosition)
    @assert !pos.positional
    @assert pos.idx == 0
    @assert pos.kw != :NULL
    return pos.kw
end

mutable struct Argument
    pos::ArgPosition
    value
end
Argument(pos::Integer, value) = Argument(ArgPosition(true, pos, :NULL), value)
Argument(kw::Symbol, value) = Argument(ArgPosition(false, 0, kw), value)
ispositional(arg::Argument) = ispositional(arg.pos)
iskw(arg::Argument) = iskw(arg.pos)
pos_idx(arg::Argument) = pos_idx(arg.pos)
pos_kw(arg::Argument) = pos_kw(arg.pos)
raw_position(arg::Argument) = raw_position(arg.pos)
value(arg::Argument) = arg.value
valuetype(arg::Argument) = typeof(arg.value)
Base.iterate(arg::Argument) = (arg.pos, true)
function Base.iterate(arg::Argument, state::Bool)
    if state
        return (arg.value, false)
    else
        return nothing
    end
end
Base.copy(arg::Argument) = Argument(ArgPosition(arg.pos), arg.value)
chunktype(arg::Argument) = chunktype(value(arg))

mutable struct TypedArgument{T}
    pos::ArgPosition
    value::T
end
TypedArgument(pos::Integer, value::T) where T = TypedArgument{T}(ArgPosition(true, pos, :NULL), value)
TypedArgument(kw::Symbol, value::T) where T = TypedArgument{T}(ArgPosition(false, 0, kw), value)
Base.setproperty!(arg::TypedArgument, name::Symbol, value::T) where T =
    throw(ArgumentError("Cannot set properties of TypedArgument"))
ispositional(arg::TypedArgument) = ispositional(arg.pos)
iskw(arg::TypedArgument) = iskw(arg.pos)
pos_idx(arg::TypedArgument) = pos_idx(arg.pos)
pos_kw(arg::TypedArgument) = pos_kw(arg.pos)
raw_position(arg::TypedArgument) = raw_position(arg.pos)
value(arg::TypedArgument) = arg.value
valuetype(arg::TypedArgument{T}) where T = T
Base.iterate(arg::TypedArgument) = (arg.pos, true)
function Base.iterate(arg::TypedArgument, state::Bool)
    if state
        return (arg.value, false)
    else
        return nothing
    end
end
Base.copy(arg::TypedArgument{T}) where T = TypedArgument{T}(ArgPosition(arg.pos), arg.value)
chunktype(arg::TypedArgument) = chunktype(value(arg))

Argument(arg::TypedArgument) = Argument(arg.pos, arg.value)

const AnyArgument = Union{Argument, TypedArgument}