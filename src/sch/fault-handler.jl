"""
    handle_fault(...)

An internal function to handle a worker dying or being killed by the OS.
Attempts to determine which `Thunk`s require rescheduling based on a
"deadlist", and then corrects the scheduler's internal `ComputeState` struct
to recover from the fault.

Note: The logic for this functionality is not currently perfectly robust to
all failure modes, and is only really intended as a last-ditch attempt to
repair and continue executing. While it should never cause incorrect execution
of DAGs, it *may* cause a `KeyError` or other failures in the scheduler due to
the complexity of getting the internal state back to a consistent and proper
state.
"""
function handle_fault(ctx, state, thunk, oldproc)
    # Find thunks whose results were cached on the dead worker and place them
    # on what's called a "deadlist". This structure will direct the recovery
    # of the scheduler's state.
    deadlist = Thunk[thunk]
    # This thunk is guaranteed to not have valid cached data anymore
    thunk.cache = false
    thunk.cache_ref = nothing
    for t in keys(state.cache)
        v = state.cache[t]
        if v isa Chunk && v.handle isa DRef && v.handle.owner == oldproc.pid
            push!(deadlist, t)
            # Any inputs to dead cached thunks must be rescheduled
            function bfs!(deadlist, t)
                for input in t.inputs
                    istask(input) || continue
                    !(input in deadlist) && push!(deadlist, input)
                    bfs!(deadlist, input)
                end
            end
            bfs!(deadlist, t)
        end
    end
    # TODO: Find *all* thunks who were actively running on the dead worker

    # Empty cache of dead thunks
    for ct in keys(state.cache)
        if ct in deadlist
            delete!(state.cache, ct)
        end
    end

    function fix_waitdicts!(state, deadlist, t::Thunk; isleaf=false)
        waiting, waiting_data = state.waiting, state.waiting_data
        if !(t in keys(waiting))
            waiting[t] = Set{Thunk}()
        end
        if !isleaf
            # If we aren't a leaf thunk, then we may still need to recover
            # further into the DAG
            for input in t.inputs
                istask(input) || continue
                @assert haskey(waiting, t) "Error: $t not in state.waiting"
                push!(waiting[t], input)
                push!(waiting_data[input], t)
                isleaf = !(input in deadlist)
                fix_waitdicts!(state, deadlist, input; isleaf=isleaf)
            end
        end
        if isempty(waiting[t])
            delete!(waiting, t)
        end
    end

    # Add state.waiting deps back to state.waiting
    for ot in keys(state.waiting)
        fix_waitdicts!(state, deadlist, ot)
    end

    fix_waitdicts!(state, deadlist, thunk)

    # Remove thunks from state.ready that have inputs on the deadlist
    for idx in length(state.ready):-1:1
        rt = state.ready[idx]
        if any((input in deadlist) for input in rt.inputs)
            deleteat!(state.ready, idx)
        end
    end

    # Remove dead thunks from state.running, and add state.running
    # deps back to state.waiting
    wasrunning = copy(state.running)
    empty!(state.running)
    while !isempty(wasrunning)
        temp = pop!(wasrunning)
        if temp isa Thunk
            if !(temp in deadlist)
                push!(state.running, temp)
            end
            fix_waitdicts!(state, deadlist, temp)
        elseif temp isa Vector
            newtemp = []
            for t in temp
                fix_waitdicts!(state, deadlist, t)
                if !(t in deadlist)
                    push!(newtemp, t)
                end
            end
            isempty(newtemp) || push!(state.running, newtemp)
        else
            throw("Unexpected type in recovery: $temp")
        end
    end

    # Reschedule inputs from deadlist
    @assert !isempty(procs(ctx)) "No workers left for fault handling!"
    while length(deadlist) > 0
        dt = popfirst!(deadlist)
        if any((input in deadlist) for input in dt.inputs)
            # We need to schedule our input thunks first
            continue
        end
        push!(state.ready, dt)
    end
    schedule!(ctx, state)
end
