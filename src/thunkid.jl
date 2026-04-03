"Identifies a thunk by its ID, and preserves the thunk in the scheduler."
struct ThunkID
    id::TaskID
    ref::Union{DRef,Nothing}
end
ThunkID(id::TaskID) = ThunkID(id, nothing)
istask(::ThunkID) = true
is_task_local(t::ThunkID) = t.id.worker == myid()

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
is_task_local(t::ThunkSyncdep) = is_task_local(t.id)