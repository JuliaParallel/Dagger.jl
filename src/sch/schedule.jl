Base.@kwdef struct TaskSchedule
    to_assign::Vector{Thunk} = Vector{Thunk}()
    assignments::Dict{Thunk,Processor} = Dict{Thunk,Processor}()
    launch_groups::Dict{Processor,Vector{Thunk}} = Dict{Processor,Vector{Thunk}}()
    failed_scheduling::Vector{Thunk} = Vector{Thunk}()
end

function schedule!(ctx, state, procs::Vector{<:Processor}=procs_to_use(ctx))
    @lock state.lock begin
        safepoint(state)

        # Schedule ready tasks
        task_sched = TaskSchedule(;to_assign=state.ready)
        schedule!(ctx, state, procs, task_sched)

        # Tasks to schedule
        to_fire = Dict{Processor,Vector{Tuple{Thunk,<:Any,<:Any,UInt64,UInt32}}}()
        for (proc, tasks) in task_sched.launch_groups
            for task in tasks
                # FIXME: Don't recompute these
                sig = signature(task, state)
                options = Options(ctx.options)
                Dagger.options_merge!(options, task.options)
                Dagger.populate_defaults!(options, sig)
                scope = if task.f isa Chunk
                    task.f.scope
                else
                    if options.proclist !== nothing
                        # proclist overrides scope selection
                        AnyScope()
                    else
                        DefaultScope()
                    end
                end
                #=has_cap,=# est_time_util, est_alloc_util, est_occupancy =
                    task_utilization(state, proc, options, sig)
                if true#has_cap
                    # Schedule task onto proc
                    proc_tasks = get!(to_fire, proc) do
                        Vector{Tuple{Thunk,<:Any,<:Any,UInt64,UInt32}}()
                    end
                    push!(proc_tasks, (task, scope, est_time_util, est_alloc_util, est_occupancy))
                end
            end
        end

        # Fire all newly-scheduled tasks
        for (proc, tasks) in to_fire
            gproc = get_parent(proc)
            fire_tasks!(ctx, tasks, (gproc, proc), state)
        end

        # Reload failed tasks back into ready queue
        append!(state.ready, task_sched.failed_scheduling)
    end
end
function schedule!(ctx, state, procs::Vector{<:Processor}, task_sched::TaskSchedule)
    @assert length(procs) > 0

    # Select a new task and get its options
    task = nothing
    @label pop_task
    if task !== nothing
        timespan_finish(ctx, :schedule, (;thunk_id=task.id), (;thunk_id=task.id))
    end
    if isempty(task_sched.to_assign)
        return
    end
    task = pop!(task_sched.to_assign)
    timespan_start(ctx, :schedule, (;thunk_id=task.id), (;thunk_id=task.id))

    # Check if this task already has a result
    if (result = cache_lookup(state, task)) !== nothing
        if haskey(state.errored, task)
            # An error was eagerly propagated to this task
            finish_failed!(state, task)
        else
            # This shouldn't have happened
            iob = IOBuffer()
            println(iob, "Scheduling inconsistency: Task being scheduled is already cached!")
            println(iob, "  Task: $(task.id)")
            println(iob, "  Cache Entry: $(typeof(result))")
            ex = SchedulingException(String(take!(iob)))
            cache_store!(state, task, ex, true)
        end
        @goto pop_task
    end

    # Fetch all inputs from cache
    inputs = collect_task_inputs(state, task)
    sig = signature(task, state)

    # Generate concrete options
    options = Options(ctx.options)
    Dagger.options_merge!(options, task.options)
    Dagger.populate_defaults!(options, sig)

    # Calculate initial task scope
    scope = if task.f isa Chunk
        task.f.scope
    else
        if options.proclist !== nothing
            # proclist overrides scope selection
            AnyScope()
        else
            DefaultScope()
        end
    end

    # Filter out Chunks
    chunks = Chunk[]
    for input in Iterators.map(last, inputs)
        if input isa Chunk
            push!(chunks, input)
        end
    end

    # Refine scope, and validate that Chunk scopes are compatible
    for chunk in chunks
        scope = constrain(scope, chunk.scope)
        if scope isa Dagger.InvalidScope
            ex = SchedulingException("Scopes are not compatible: $(scope.x), $(scope.y)")
            cache_store!(state, task, ex, true)
            set_failed!(state, task)
            @goto pop_task
        end
    end

    # Collect all known processors
    all_procs = unique(vcat([collect(Dagger.get_processors(gp)) for gp in procs]...))

    # Decide on an ordered set of candidate processors to schedule on
    schedule_model = @something(options.schedule_model, state.schedule_model)
    local_procs = make_decision(schedule_model, :signature, :schedule, sig, inputs, all_procs)

    # Select the first valid processor
    scheduled = false
    for proc in local_procs
        gproc = get_parent(proc)
        can_use, scope = can_use_proc(task, gproc, proc, options, scope)
        if can_use
            launch_group = get!(Vector{Thunk}, task_sched.launch_groups, proc)
            push!(launch_group, task)
            @dagdebug task :schedule "Scheduling to $proc"
            @goto pop_task
        end
    end

    # Report that no processors were valid
    ex = SchedulingException("No processors available, try widening scope")
    cache_store!(state, task, ex, true)
    set_failed!(state, task)
    @goto pop_task
end
