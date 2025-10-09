"Identifies a thunk by its ID, and preserves the thunk in the scheduler."
struct ThunkID
    id::Int
    ref::Union{DRef,Nothing}
end
ThunkID(id::Int) = ThunkID(id, nothing)
istask(::ThunkID) = true

struct ThunkSyncdep
    id::Union{ThunkID,Nothing}
    thunk
end
ThunkSyncdep() = ThunkSyncdep(nothing, nothing)
ThunkSyncdep(id::ThunkID) = ThunkSyncdep(id, nothing)
ThunkSyncdep(x) = convert(ThunkSyncdep, x)
Base.getindex(syncdep::ThunkSyncdep) = @something(syncdep.id, syncdep.thunk)
Base.convert(::Type{ThunkSyncdep}, id::ThunkID) = ThunkSyncdep(id, nothing)
unwrap_weak(t::ThunkSyncdep) = unwrap_weak(t.thunk)
istask(::ThunkSyncdep) = true
