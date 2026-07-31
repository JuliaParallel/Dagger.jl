using StructTypes, JSON3

sanitize(t::Tuple) = map(sanitize, t)
sanitize(t::NamedTuple) = map(sanitize, t)
sanitize(x::Function) = repr(x)
sanitize(d::Dict) = Dict(sanitize(k)=>sanitize(d[k]) for k in keys(d))
sanitize(a::Array) = sanitize.(a)
sanitize(s::Set) = Set(sanitize.(s))
sanitize(x) = x

StructTypes.StructType(::Type{<:Dagger.Processor}) = StructTypes.CustomStruct()
StructTypes.lower(proc::Dagger.Processor) = repr(proc)
StructTypes.StructType(::Type{<:Dagger.Chunk}) = StructTypes.CustomStruct()
StructTypes.lower(chunk::Dagger.Chunk) = repr(chunk)
StructTypes.StructType(::Type{<:TimespanLogging.Event}) = StructTypes.CustomStruct()
function StructTypes.lower(ev::TimespanLogging.Event{T}) where T
    kind = T
    category = ev.category
    timestamp = ev.timestamp
    id = ev.id
    timeline = if kind == :move
        sanitize((;f=ev.timeline.f,id=ev.timeline.id))
    elseif kind == :evict
        (;)
    else
        sanitize(ev.timeline)
    end
    return (;kind, category, timestamp, id, timeline)
end
