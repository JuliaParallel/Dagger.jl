module JuMPExt

if isdefined(Base, :get_extension)
    using JuMP
else
    using ..JuMP
end

using Dagger
import Dagger: JuMPScheduler, DAGSpec, datadeps_schedule_dag_aot!,
               proc_in_scope, DefaultScope,
               _eft_runtime_ns, _milp_edge_size_bytes, _milp_transfer_time_ns,
               GREEDY_DEFAULT_RUNTIME_NS,
               ScheduleState, greedy_schedule!
import Dagger.Sch
import MetricsTracker as MT
import Graphs: nv, outdegree, edges, src, dst

function _milp_compatible_procs(spec, all_procs::Vector{Dagger.Processor})
    task_scope = @something(spec.options.compute_scope, spec.options.scope, DefaultScope())
    return filter(p -> proc_in_scope(p, task_scope), all_procs)
end

function Dagger.datadeps_schedule_dag_aot!(sched::JuMPScheduler, schedule, dag_spec, all_procs, all_scope)
    n_tasks = nv(dag_spec.g)
    n_tasks == 0 && return

    snap = MT.snapshot(MT.global_metrics_cache())
    nprocs = length(all_procs)

    # Cost matrix uses the same EFT fallback as the heuristics so MILP
    # optimises against an identical cost model.
    task_times = zeros(Float64, n_tasks, nprocs)
    proc_compatible = falses(n_tasks, nprocs)
    @inbounds for k in 1:n_tasks
        spec = dag_spec.id_to_spec[k]
        compatible = _milp_compatible_procs(spec, all_procs)
        if isempty(compatible)
            throw(Sch.SchedulingException("JuMPScheduler: no compatible processor for task $k"))
        end
        for (w, proc) in enumerate(all_procs)
            if proc in compatible
                proc_compatible[k, w] = true
                task_times[k, w] = _eft_runtime_ns(snap, spec, proc)
            else
                task_times[k, w] = Float64(GREEDY_DEFAULT_RUNTIME_NS)
            end
        end
    end

    edge_list = [(src(e), dst(e)) for e in edges(dag_spec.g)]
    γ = Dict{Tuple{Int, Int}, Matrix{Float64}}()
    for (k, l) in edge_list
        size_bytes = _milp_edge_size_bytes(dag_spec, k, l)
        mat = zeros(Float64, nprocs, nprocs)
        @inbounds for w1 in 1:nprocs, w2 in 1:nprocs
            w1 == w2 && continue
            src_space = only(Dagger.memory_spaces(all_procs[w1]))
            dst_space = only(Dagger.memory_spaces(all_procs[w2]))
            mat[w1, w2] = _milp_transfer_time_ns(snap, src_space, dst_space, size_bytes)
        end
        γ[(k, l)] = mat
    end

    # Greedy seed serves as MIP warm-start (V&S 2015 §6.1): known incumbent
    # for BB pruning, and a fallback feasible solution on time-limit truncation.
    seed_state = ScheduleState()
    greedy_schedule!(seed_state, snap, dag_spec, all_procs)

    # Big-M = sum_k max_w c_{kw} is a provable upper bound on any feasible
    # makespan (all-serial on slowest procs). Tighter than DagScheduler.jl's
    # 1.5× multiplier, improves the LP relaxation.
    big_m = sum(maximum(view(task_times, k, :)) for k in 1:n_tasks; init=0.0)
    big_m = max(big_m, 1.0)

    model = Model(sched.optimizer)
    JuMP.set_silent(model)
    JuMP.set_time_limit_sec(model, sched.time_limit_sec)

    @variable(model, 0 <= t[1:n_tasks] <= big_m)
    @variable(model, 0 <= t_last_end <= big_m)
    @variable(model, s[1:n_tasks, 1:nprocs], Bin)

    # Only binary values are warm-started; solver fills continuous vars via
    # LP relaxation. Matches the MIP-start protocol of HiGHS, Gurobi, CPLEX.
    @inbounds for k in 1:n_tasks
        seed_proc_idx = findfirst(==(seed_state.task_proc[k]), all_procs)
        for w in 1:nprocs
            JuMP.set_start_value(s[k, w], w == seed_proc_idx ? 1.0 : 0.0)
        end
    end

    @constraint(model, [k in 1:n_tasks], sum(s[k, :]) == 1)

    @inbounds for k in 1:n_tasks, w in 1:nprocs
        if !proc_compatible[k, w]
            @constraint(model, s[k, w] == 0)
        end
    end

    if !isempty(edge_list)
        edge_max_γ = Dict((k, l) => maximum(γ[(k, l)]) for (k, l) in edge_list)
        @variable(model, 0 <= p[(k, l) in edge_list] <= edge_max_γ[(k, l)])

        for (k, l) in edge_list
            γkl = γ[(k, l)]
            for w1 in 1:nprocs, w2 in 1:nprocs
                w1 == w2 && continue
                γkl[w1, w2] == 0 && continue    # redundant with p >= 0
                @constraint(model, p[(k, l)] >= (s[k, w1] + s[l, w2] - 1) * γkl[w1, w2])
            end

            @constraint(model,
                t[k] + sum(task_times[k, w] * s[k, w] for w in 1:nprocs) + p[(k, l)] <= t[l])
        end
    end

    for l in 1:n_tasks
        outdegree(dag_spec.g, l) == 0 || continue
        @constraint(model,
            t[l] + sum(task_times[l, w] * s[l, w] for w in 1:nprocs) <= t_last_end)
    end

    # Sequential big-M: enforces same-proc serialisation only for (k,l) pairs
    # with k < l and no direct edge (edges get precedence-with-penalty above).
    edge_set = Set(edge_list)
    for k in 1:(n_tasks - 1), l in (k + 1):n_tasks
        ((k, l) in edge_set || (l, k) in edge_set) && continue
        for w in 1:nprocs
            @constraint(model,
                t[k] + sum(task_times[k, w2] * s[k, w2] for w2 in 1:nprocs) <=
                    t[l] + big_m * (2 - s[k, w] - s[l, w]))
        end
    end

    # sum(t) and sum(p) tie-break equally-optimal makespans toward earlier
    # starts and lower cross-proc traffic (matches DagScheduler.jl).
    if !isempty(edge_list)
        @objective(model, Min, sched.Z * t_last_end + sum(t) + sum(p))
    else
        @objective(model, Min, sched.Z * t_last_end + sum(t))
    end

    optimize!(model)

    status = termination_status(model)
    if !(status == MOI.OPTIMAL || status == MOI.TIME_LIMIT)
        throw(Sch.SchedulingException("JuMPScheduler: solver returned $status; no feasible schedule found"))
    end
    if status == MOI.TIME_LIMIT && primal_status(model) != MOI.FEASIBLE_POINT
        throw(Sch.SchedulingException("JuMPScheduler: time limit $(sched.time_limit_sec)s hit with no feasible schedule found"))
    end

    s_vals = value.(s)
    @inbounds for k in 1:n_tasks
        proc_idx = findfirst(j -> s_vals[k, j] > 0.5, 1:nprocs)
        if proc_idx === nothing
            throw(Sch.SchedulingException("JuMPScheduler: solver returned no processor assignment for task $k"))
        end
        task = dag_spec.id_to_task[k]
        schedule[task] = all_procs[proc_idx]
    end
    return
end

end # module JuMPExt
