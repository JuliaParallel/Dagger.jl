"""
    handle_fault(...)

An internal function to handle a worker dying or being killed by the OS.
Attempts to determine which `Thunk`s were running on (or had their results
cached on) the dead worker, and stores them in a "deadlist". It uses this
deadlist to correct the scheduler's internal `ComputeState` struct to recover
from the fault.

Note: The logic for this functionality is not currently perfectly robust to
all failure modes, and is only really intended as a last-ditch attempt to
repair and continue executing. While it should never cause incorrect execution
of DAGs, it *may* cause a `KeyError` or other failures in the scheduler due to
the complexity of getting the internal state back to a consistent and proper
state.
"""
function handle_fault(ctx, state, deadproc)
    @assert !isempty(procs(ctx)) "No workers left for fault handling!"

    deadlist = Thunk[]

    # Evict cache entries that were stored on the worker
    for t in keys(state.cache)
        v = state.cache[t]
        if v isa Chunk && v.handle isa DRef && v.handle.owner == deadproc.pid
            push!(deadlist, t)
            pop!(state.cache, t)
        end
    end
    # Remove thunks that were running on the worker
    for t in collect(keys(state.running_on))
        pid = state.running_on[t].pid
        if pid == deadproc.pid
            push!(deadlist, t)
            delete!(state.running_on, t)
            pop!(state.running, t)
        end
    end
    # Clear thunk.cache_ref
    for t in deadlist
        t.cache_ref = nothing
    end

    # Remove thunks from state.ready that have inputs on the deadlist
    for idx in length(state.ready):-1:1
        rt = state.ready[idx]
        if any((input in deadlist) for input in map(last, rt.inputs))
            deleteat!(state.ready, idx)
        end
    end

    # Reschedule inputs from deadlist
    seen = Set{Thunk}()
    for t in deadlist
        reschedule_syncdeps!(state, t, seen)
    end
end
