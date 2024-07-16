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
