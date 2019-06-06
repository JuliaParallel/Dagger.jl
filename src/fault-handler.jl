"Recursively checks if an exception was caused by a worker exiting."
check_exited_exception(res::CapturedException) =
    check_exited_exception(res.ex)
check_exited_exception(res::RemoteException) =
    check_exited_exception(res.captured)
check_exited_exception(res::ProcessExitedException) = true
check_exited_exception(res) = false

function handle_fault(ctx, state, thunk, oldproc, chan, node_order)
    # Find thunks whose results were cached on the dead worker
    deadlist = Thunk[thunk]
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

    function fix_waitdicts!(state, deadlist, t::Thunk; isleaf=false, offset=0)
        waiting, waiting_data = state.waiting, state.waiting_data
        off = repeat(" ", offset)
        offi = repeat(" ", offset+1)
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
                fix_waitdicts!(state, deadlist, input; isleaf=isleaf, offset=offset+1)
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
    newproc = OSProc(rand(workers()))
    while length(deadlist) > 0
        dt = popfirst!(deadlist)
        if any((input in deadlist) for input in dt.inputs)
            # We need to schedule our input thunks first
            push!(deadlist, dt)
            continue
        end
        fire_task!(ctx, dt, newproc, state, chan, node_order)
        break
    end
end
