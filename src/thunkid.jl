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

# Pool of syncdeps Sets for datadeps planning. Every planned task allocates a
# Set{ThunkSyncdep} (plus its internal arrays); on the synchronous submission
# path the set is provably dead once eager submission has consumed it (its
# elements are rewritten to WeakThunk form and only debug printers look at it
# afterwards), so datadeps reclaims it right after enqueue. Global and locked:
# the planner and any recycler may be different tasks.
const SYNCDEPS_SET_POOL = LockedObject(Vector{Set{ThunkSyncdep}}())
const SYNCDEPS_SET_POOL_CAP = 4096
function take_syncdeps_set!()
    set = @safe_lock_spin1 SYNCDEPS_SET_POOL pool begin
        isempty(pool) ? nothing : pop!(pool)
    end
    set === nothing || return set::Set{ThunkSyncdep}
    return Set{ThunkSyncdep}()
end
function return_syncdeps_set!(set::Set{ThunkSyncdep})
    empty!(set)
    @safe_lock_spin1 SYNCDEPS_SET_POOL pool begin
        length(pool) < SYNCDEPS_SET_POOL_CAP && push!(pool, set)
    end
    return
end

"""
    syncdeps_consumed(set) -> Bool

Whether eager submission has already consumed this syncdeps set: submission
drains the planner-recorded `ThunkID`-form entries and refills the set with
resolved `WeakThunk`-form entries, so all-WeakThunk (or empty) means the last
real reader is done. Deferred submission (`launch_wait`/batched enqueue)
leaves the planner form in place, and callers must then NOT reclaim the set.
"""
syncdeps_consumed(set::Set{ThunkSyncdep}) = all(dep->dep.thunk isa WeakThunk, set)
