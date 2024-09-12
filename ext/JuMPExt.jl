module JuMPExt

if isdefined(Base, :get_extension)
    using JuMP
else
    using ..JuMP
end

using Dagger
using Dagger.Distributed
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

    time_costs = remotecall_fetch(1) do
        Dagger.Sch.init_eager()
        sch_state = Dagger.Sch.EAGER_STATE[]
        return deepcopy(sch_state.signature_time_cost)
    end
    task_times = zeros(UInt, ntasks, nprocs)
    for (spec, task) in specs_tasks
        id = task_to_id[task]
        for p in 1:nprocs
            # FIXME: Use whatever estimate is available and closest if not
            # populated for this processor
            #sig = Dagger.Sch.signature(spec, task)
            # FIXME: Dict concrete types
            #task_times[id, p] = get(get(time_costs, sig, Dict()), p, UInt(0))
            task_times[id, p] = UInt(rand(1:10))
        end
    end

    xfer_times = zeros(Int, nprocs, nprocs)
    # FIXME: Actually fill this with estimated xfer times
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
    # Penalties for moving between procs
    @variable(m, p[a_kls] >= 0)

    # Each task is assigned to exactly one processor
    @constraint(m, [k in 1:ntasks], sum(s[k, :]) == 1)

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

    for l in filter(n -> outdegree(g, n) == 0, 1:nv(g))
        # DAG ends after the last task
        @constraint(m, t[l] + task_times[l, :]' * s[l, :] <= t_last_end)
    end

    # Minimize the total runtime of the DAG
    # TODO: Do we need to bias towards earlier start times?
    @objective(m, Min, sched.Z*t_last_end + sum(t) .+ sum(p))

    # Solve the model
    @time optimize!(m)

    # Extract the schedule from the model
    task_to_proc = Dict{DTask, Dagger.Processor}()
    for k in 1:ntasks
        proc_id = findfirst(identity, value.(s[k, :]) .== 1)
        task_to_proc[id_to_task[k]] = id_to_proc[proc_id]
    end

    return task_to_proc
end

end # module JuMPExt
