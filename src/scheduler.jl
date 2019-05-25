module Sch

using Distributed
import MemPool: DRef

import ..Dagger: Context, Thunk, Chunk, OSProc, order, free!, dependents, noffspring, istask, inputs, affinity, tochunk, @dbg, @logmsg, timespan_start, timespan_end, unrelease, procs

const OneToMany = Dict{Thunk, Set{Thunk}}
struct ComputeState
    dependents::OneToMany
    finished::Set{Thunk}
    waiting::OneToMany
    waiting_data::OneToMany
    ready::Vector{Thunk}
    cache::Dict{Thunk, Any}
    running::Set{Thunk}
    thunk_dict::Dict{Int, Any}
end

struct ComputeOptions
    single::Int
end
ComputeOptions() = ComputeOptions(0)

function cleanup(ctx)
end

function compute_dag(ctx, d::Thunk; options=ComputeOptions())
    if options === nothing
        options = ComputeOptions()
    end
    master = OSProc(myid())
    @dbg timespan_start(ctx, :scheduler_init, 0, master)

    if options.single !== 0
        @assert options.single in vcat(1, workers()) "Sch option 'single' must specify an active worker id"
        ps = OSProc[OSProc(options.single)]
    else
        ps = procs(ctx)
    end
    chan = Channel{Any}(32)
    deps = dependents(d)
    ord = order(d, noffspring(deps))

    node_order = x -> -get(ord, x, 0)
    state = start_state(deps, node_order)
    # start off some tasks
    for p in ps
        isempty(state.ready) && break
        task = pop_with_affinity!(ctx, state.ready, p, false)
        if task !== nothing
            fire_task!(ctx, task, p, state, chan, node_order)
        end
    end
    @dbg timespan_end(ctx, :scheduler_init, 0, master)

    while !isempty(state.ready) || !isempty(state.running)
        if isempty(state.running) && !isempty(state.ready)
            for p in ps
                isempty(state.ready) && break
                task = pop_with_affinity!(ctx, state.ready, p, false)
                if task !== nothing
                    fire_task!(ctx, task, p, state, chan, node_order)
                end
            end
        end

        if isempty(state.running)
            # the block above fired only meta tasks
            continue
        end

        function check_exited_exception(res::CapturedException)
            check_exited_exception(res.ex)
        end
        function check_exited_exception(res::RemoteException)
            check_exited_exception(res.captured)
        end
        check_exited_exception(res::ProcessExitedException) = true
        check_exited_exception(res) = false

        proc, thunk_id, res = take!(chan)
        #@debug "Took thunk $thunk_id"
        if isa(res, CapturedException) || isa(res, RemoteException)
            if check_exited_exception(res)
                @warn "Worker $(proc.pid) died on thunk $thunk_id, rescheduling work"
                # TODO: showerror(res)
                thunk = state.thunk_dict[thunk_id]

                # Remove dead worker from procs list
                filter!(p->p.pid!=proc.pid, ctx.procs)
                ps = procs(ctx)

                #=
                @debug "Pre-recovery State:"
                @show state.ready
                @show state.running
                @show keys(state.cache)
                @show state.waiting
                @show state.waiting_data
                =#

                # Find thunks whose results were cached on the dead worker
                deadlist = Thunk[thunk]
                thunk.cache = false
                thunk.cache_ref = nothing
                for t in keys(state.cache)
                    v = state.cache[t]
                    if v isa Chunk && v.handle isa DRef && v.handle.owner == proc.pid
                        @warn "Found dead cached thunk to reschedule: $t"
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
                #@debug "Deadlist: $deadlist"
                # TODO: Find thunks who were actively running on the dead worker

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
                    #@debug "$off Fixing $t"
                    if !(t in keys(waiting))
                        #@debug "Not in waiting: $t"
                        waiting[t] = Set{Thunk}()
                    end
                    if !isleaf
                        # If we aren't a leaf thunk, then we may still need to
                        # recover further into the DAG
                        #@debug "$off Begin fix inputs for $t: $(t.inputs)"
                        for input in t.inputs
                            #@debug "$offi istask input $input of $t: $(istask(input))"
                            istask(input) || continue
                            #@debug "$offi Add input to waiting for $t: $input"
                            #@show state.waiting
                            @assert haskey(waiting, t) "Error: $t not in waiting"
                            push!(waiting[t], input)
                            push!(waiting_data[input], t)
                            isleaf = !(input in deadlist)
                            fix_waitdicts!(state, deadlist, input; isleaf=isleaf, offset=offset+1)
                        end
                        #@debug "$off End fix inputs for $t"
                    end
                    if isempty(waiting[t])
                        #@debug "Prune waiting: $t"
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
                # FIXME: If a thunk is actively running on a live node, it may
                # fail during/after this recovery. Is that a problem?
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
                        throw("Unexpected $temp")
                    end
                end

                #=
                @debug "Recovering State:"
                @show state.ready
                @show state.running
                @show keys(state.cache)
                @show state.waiting
                @show state.waiting_data
                =#

                # Reschedule inputs from deadlist
                newproc = OSProc(rand(workers()))
                while length(deadlist) > 0
                    dt = popfirst!(deadlist)
                    if any((input in deadlist) for input in dt.inputs)
                        # We need to schedule our input thunks first
                        push!(deadlist, dt)
                        continue
                    end
                    @warn "Re-scheduling $dt"
                    fire_task!(ctx, dt, newproc, state, chan, node_order)
                    break
                end

                continue
            else
                throw(res)
            end
        end
        node = state.thunk_dict[thunk_id]
        @logmsg("WORKER $(proc.pid) - $node ($(node.f)) input:$(node.inputs)")
        state.cache[node] = res

        #=
        @debug "Normal State:"
        @show state.ready
        @show state.running
        @show keys(state.cache)
        @show state.waiting
        @show state.waiting_data
        =#

        @dbg timespan_start(ctx, :scheduler, thunk_id, master)
        immediate_next = finish_task!(state, node, node_order)
        if !isempty(state.ready)
            thunk = pop_with_affinity!(Context(ps), state.ready, proc, immediate_next)
            if thunk !== nothing
                #@debug "End-firing $thunk"
                fire_task!(ctx, thunk, proc, state, chan, node_order)
            end
        end
        @dbg timespan_end(ctx, :scheduler, thunk_id, master)
    end
    state.cache[d]
end

function pop_with_affinity!(ctx, tasks, proc, immediate_next)
    # allow JIT specialization on Pairs
    mapfirst(c) = first.(c)

    if immediate_next
        # fast path
        if proc in mapfirst(affinity(tasks[end]))
            return pop!(tasks)
        end
    end

    # TODO: use the size
    parent_affinity_procs = Vector(undef, length(tasks))
    # parent_affinity_sizes = Vector(undef, length(tasks))
    for i=length(tasks):-1:1
        t = tasks[i]
        aff = affinity(t)
        aff_procs = mapfirst(aff)
        if proc in aff_procs
            deleteat!(tasks, i)
            return t
        end
        parent_affinity_procs[i] = aff_procs
    end
    for i=length(tasks):-1:1
        # use up tasks without affinities
        # let the procs with the respective affinities pick up
        # other tasks
        aff_procs = parent_affinity_procs[i]
        if isempty(aff_procs)
            t = tasks[i]
            deleteat!(tasks, i)
            return t
        end
        if all(!(p in aff_procs) for p in procs(ctx))
            # no proc is ever going to ask for it
            t = tasks[i]
            deleteat!(tasks, i)
            return t
        end
    end
    return nothing
end

function fire_task!(ctx, thunk, proc, state, chan, node_order)
    @logmsg("W$(proc.pid) + $thunk ($(showloc(thunk.f, length(thunk.inputs)))) input:$(thunk.inputs) cache:$(thunk.cache) $(thunk.cache_ref)")
    push!(state.running, thunk)
    if thunk.cache && thunk.cache_ref !== nothing
        # the result might be already cached
        data = unrelease(thunk.cache_ref) # ask worker to keep the data around
                                          # till this compute cycle frees it
        if data !== nothing
            @logmsg("cache hit: $(thunk.cache_ref)")
            state.cache[thunk] = data
            immediate_next = finish_task!(state, thunk, node_order; free=false)
            if !isempty(state.ready)
                thunk = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
                if thunk !== nothing
                    fire_task!(ctx, thunk, proc, state, chan, node_order)
                end
            end
            return
        else
            thunk.cache_ref = nothing
            @logmsg("cache miss: $(thunk.cache_ref) recomputing $(thunk)")
        end
    end

    if thunk.meta
        # Run it on the parent node
        # do not _move data.
        p = OSProc(myid())
        @dbg timespan_start(ctx, :comm, thunk.id, p)
        fetched = map(thunk.inputs) do x
            istask(x) ? state.cache[x] : x
        end
        @dbg timespan_end(ctx, :comm, thunk.id, p)

        @dbg timespan_start(ctx, :compute, thunk.id, p)
        res = thunk.f(fetched...)
        @dbg timespan_end(ctx, :compute, thunk.id, p)

        #push!(state.running, thunk)
        state.cache[thunk] = res
        immediate_next = finish_task!(state, thunk, node_order; free=false)
        if !isempty(state.ready)
            if immediate_next
                thunk = pop!(state.ready)
            else
                thunk = pop_with_affinity!(ctx, state.ready, proc, immediate_next)
            end
            if thunk !== nothing
                fire_task!(ctx, thunk, proc, state, chan, node_order)
            end
        end
        return
    end

    data = map(thunk.inputs) do x
        istask(x) ? state.cache[x] : x
    end
    state.thunk_dict[thunk.id] = thunk
    async_apply(ctx, proc, thunk.id, thunk.f, data, chan, thunk.get_result, thunk.persist, thunk.cache)
end

function finish_task!(state, node, node_order; free=true)
    if istask(node) && node.cache
        node.cache_ref = state.cache[node]
    end
    immediate_next = false
    for dep in sort!(collect(state.dependents[node]), by=node_order)
        set = state.waiting[dep]
        pop!(set, node)
        if isempty(set)
            pop!(state.waiting, dep)
            push!(state.ready, dep)
            immediate_next = true
        end
        # todo: free data
    end
    for inp in inputs(node)
        if inp in keys(state.waiting_data)
            s = state.waiting_data[inp]
            if node in s
                pop!(s, node)
            end
            if free && isempty(s)
                if haskey(state.cache, inp)
                    _node = state.cache[inp]
                    free!(_node, force=false, cache=(istask(inp) && inp.cache))
                    pop!(state.cache, inp)
                end
            end
        end
    end
    push!(state.finished, node)
    pop!(state.running, node)
    immediate_next
end

function start_state(deps::Dict, node_order)
    state = ComputeState(
                  deps,
                  Set{Thunk}(),
                  OneToMany(),
                  OneToMany(),
                  Vector{Thunk}(undef, 0),
                  Dict{Thunk, Any}(),
                  Set{Thunk}(),
                  Dict{Int, Thunk}()
                 )

    nodes = sort(collect(keys(deps)), by=node_order)
    merge!(state.waiting_data, deps)
    for k in nodes
        if istask(k)
            waiting = Set{Thunk}(Iterators.filter(istask,
                                                  inputs(k)))
            if isempty(waiting)
                push!(state.ready, k)
            else
                state.waiting[k] = waiting
            end
        end
    end
    state
end

_move(ctx, to_proc, x) = x
_move(ctx, to_proc::OSProc, x::Union{Chunk, Thunk}) = collect(ctx, x)

@noinline function do_task(ctx, proc, thunk_id, f, data, send_result, persist, cache)
    @dbg timespan_start(ctx, :comm, thunk_id, proc)
    time_cost = @elapsed fetched = map(x->_move(ctx, proc, x), data)
    @dbg timespan_end(ctx, :comm, thunk_id, proc)

    @dbg timespan_start(ctx, :compute, thunk_id, proc)
    result_meta = try
        res = f(fetched...)
        (proc, thunk_id, send_result ? res : tochunk(res, persist=persist, cache=persist ? true : cache)) #todo: add more metadata
    catch ex
        bt = catch_backtrace()
        (proc, thunk_id, RemoteException(myid(), CapturedException(ex, bt)))
    end
    @dbg timespan_end(ctx, :compute, thunk_id, proc)
    result_meta
end

@noinline function async_apply(ctx, p::OSProc, thunk_id, f, data, chan, send_res, persist, cache)
    @async begin
        try
            put!(chan, remotecall_fetch(do_task, p.pid, ctx, p, thunk_id, f, data, send_res, persist, cache))
        catch ex
            bt = catch_backtrace()
            put!(chan, (p, thunk_id, CapturedException(ex, bt)))
        end
        nothing
    end
end

end # module Sch
