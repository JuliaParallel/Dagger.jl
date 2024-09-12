module JuMPExt

if isdefined(Base, :get_extension)
    using JuMP
else
    using ..JuMP
end

using Dagger
using Dagger.Distributed
import MetricsTracker as MT
import Graphs: edges, nv, outdegree

struct JuMPScheduler
    optimizer
    Z::Float64
    JuMPScheduler(optimizer) = new(optimizer, 10)
end
function Dagger.datadeps_create_schedule(sched::JuMPScheduler, state, specs_tasks)
    astate = state.alias_state
    g, task_to_id = astate.g, astate.task_to_id
    id_to_task = Dict(id => task for (task, id) in task_to_id)
    ntasks = length(specs_tasks)
    nprocs = length(state.all_procs)
    id_to_proc = Dict(i => p for (i, p) in enumerate(state.all_procs))

    # Estimate the time each task will take to execute on each processor,
    # and the time it will take to transfer data between processors
    task_times = zeros(UInt64, ntasks, nprocs)
    xfer_times = zeros(Int, nprocs, nprocs)
    lock(MT.GLOBAL_METRICS_CACHE) do cache
        for (spec, task) in specs_tasks
            id = task_to_id[task]
            for p in 1:nprocs
                # When searching for a task runtime estimate, we use whatever
                # estimate is available and closest if not populated for this processor
                # Exact match > same proc type, same node > same proc type, any node > any proc type

                sig = Dagger.Sch.signature(spec.f, map(pos_arg->pos_arg[1] => Dagger.unwrap_inout_value(pos_arg[2]), spec.args))
                proc = state.all_procs[p]
                @warn "Use node, not worker id!" maxlog=1
                pid = Dagger.root_worker_id(proc)

                # Try exact match
                match_on = (MT.LookupExact(Dagger.SignatureMetric(), sig),
                            MT.LookupExact(Dagger.ProcessorMetric(), proc))
                result = MT.cache_lookup(cache, Dagger, :execute!, MT.TimeMetric(), match_on)::Union{UInt64, Nothing}
                if result !== nothing
                    task_times[id, p] = result
                    continue
                end

                # Try same proc type, same node
                match_on = (MT.LookupExact(Dagger.SignatureMetric(), sig),
                            MT.LookupSubtype(Dagger.ProcessorMetric(), typeof(proc)),
                            MT.LookupCustom(Dagger.ProcessorMetric(), other_proc->Dagger.root_worker_id(other_proc)==pid))
                result = MT.cache_lookup(cache, Dagger, :execute!, MT.TimeMetric(), match_on)::Union{UInt64, Nothing}
                if result !== nothing
                    task_times[id, p] = result
                    continue
                end

                # Try same proc type, any node
                match_on = (MT.LookupExact(Dagger.SignatureMetric(), sig),
                            MT.LookupSubtype(Dagger.ProcessorMetric(), typeof(proc)))
                result = MT.cache_lookup(cache, Dagger, :execute!, MT.TimeMetric(), match_on)::Union{UInt64, Nothing}
                if result !== nothing
                    task_times[id, p] = result
                    continue
                end

                # Try any signature match
                match_on = MT.LookupExact(Dagger.SignatureMetric(), sig)
                result = MT.cache_lookup(cache, Dagger, :execute!, MT.TimeMetric(), match_on)::Union{UInt64, Nothing}
                if result !== nothing
                    task_times[id, p] = result
                    continue
                end

                # If no information is available, use a random guess
                task_times[id, p] = UInt64(rand(1:1_000_000))
            end
        end

        # FIXME: Actually fill this with estimated xfer times
        @warn "Assuming all xfer times are 1" maxlog=1
        for dst in 1:nprocs
            for src in 1:nprocs
                if src == dst # FIXME: Or if space is shared
                    xfer_times[src, dst] = 0
                else
                    # FIXME: sum(currently non-local task arg size) / xfer_speed
                    xfer_times[src, dst] = 1
                end
            end
        end
    end

    @warn "If no edges exist, this will fail" maxlog=1
    γ = Dict{Tuple{Int, Int}, Matrix{Int}}()
    for (i, j) in Tuple.(edges(g))
        γ[(i, j)] = copy(xfer_times)
    end

    a_kls = Tuple.(edges(g))
    m = Model(sched.optimizer)
    JuMP.set_silent(m)

    # Start time of each task
    @variable(m, t[1:ntasks] >= 0)
    # End time of last task
    @variable(m, t_last_end >= 0)

    # 1 if task k is assigned to proc p
    @variable(m, s[1:ntasks, 1:nprocs], Bin)

    # Each task is assigned to exactly one processor
    @constraint(m, [k in 1:ntasks], sum(s[k, :]) == 1)

    # Penalties for moving between procs
    if length(a_kls) > 0
        @variable(m, p[a_kls] >= 0)

        for (k, l) in a_kls
            for p1 in 1:nprocs
                for p2 in 1:nprocs
                    p1 == p2 && continue
                    # Task l occurs after task k if the procs are different,
                    # thus there is a penalty
                    @constraint(m, p[(k, l)] >= (s[k, p1] + s[l, p2] - 1) * γ[(k, l)][p1, p2])
                end
            end

            # Task l occurs after task k
            @constraint(m, t[k] + task_times[k, :]' * s[k, :] + p[(k, l)] <= t[l])
        end
    else
        @variable(m, p >= 0)
    end

    for l in filter(n -> outdegree(g, n) == 0, 1:nv(g))
        # DAG ends after the last task
        @constraint(m, t[l] + task_times[l, :]' * s[l, :] <= t_last_end)
    end

    # Minimize the total runtime of the DAG
    # TODO: Do we need to bias towards earlier start times?
    @objective(m, Min, sched.Z*t_last_end + sum(t) .+ sum(p))

    # Solve the model
    optimize!(m)

    # Extract the schedule from the model
    task_to_proc = Dict{DTask, Dagger.Processor}()
    for k in 1:ntasks
        proc_id = findfirst(identity, value.(s[k, :]) .== 1)
        task_to_proc[id_to_task[k]] = id_to_proc[proc_id]
    end

    return task_to_proc
end

end # module JuMPExt
