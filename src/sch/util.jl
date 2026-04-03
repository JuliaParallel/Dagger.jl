"Like `errormonitor`, but tracks how many outstanding tasks are running."
function errormonitor_tracked(name::String, t::Task)
    @safe_lock_spin1 ERRORMONITOR_TRACKED tracked begin
        push!(tracked, name => t)
    end
    errormonitor(Threads.@spawn begin
        try
            wait(t)
        finally
            lock(ERRORMONITOR_TRACKED) do tracked
                idx = findfirst(o->o[2]===t, tracked)
                # N.B. This may be nothing if precompile emptied these
                if idx !== nothing
                    deleteat!(tracked, idx)
                end
            end
        end
    end)
end
function errormonitor_tracked_set!(name::String, t::Task)
    lock(ERRORMONITOR_TRACKED) do tracked
        for idx in 1:length(tracked)
            if tracked[idx][2] === t
                tracked[idx] = name => t
                return
            end
        end
        error("Task not found in tracked list")
    end
end
const ERRORMONITOR_TRACKED = LockedObject(Pair{String,Task}[])

"""
    unwrap_nested_exception(err::Exception) -> Bool

Extracts the "core" exception from a nested exception."
"""
unwrap_nested_exception(err::CapturedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::RemoteException) =
    unwrap_nested_exception(err.captured)
unwrap_nested_exception(err::DTaskFailedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::TaskFailedException) =
    unwrap_nested_exception(err.task.result)
unwrap_nested_exception(err::LoadError) =
    unwrap_nested_exception(err.error)
unwrap_nested_exception(err) = err

"Gets a `NamedTuple` of options propagated from `options`."
function get_propagated_options(options::Options)
    # FIXME: Just use an Options as output?
    nt = NamedTuple()
    if options.propagates === nothing
        return nt
    end
    for key in options.propagates
        value = if hasfield(Options, key)
            getfield(options, key)
        else
            throw(ArgumentError("Can't propagate unknown key: $key"))
        end
        nt = merge(nt, (key=>value,))
    end
    return nt
end

has_result(thunk) = thunk.cache_ref !== nothing
has_error(thunk) = thunk.errored
function load_result(thunk)
    @assert thunk.finished "Thunk[$(thunk.id)] is not yet finished"
    return something(thunk.cache_ref)
end
function store_result!(thunk, value; error::Bool=false)
    @assert !thunk.finished "Thunk[$(thunk.id)] should not be finished yet"
    @assert !has_result(thunk) "Thunk[$(thunk.id)] already contains a cached result"
    thunk.finished = true
    if error && value isa Exception && !(value isa DTaskFailedException)
        thunk.cache_ref = Some{Any}(DTaskFailedException(thunk, thunk, value))
    else
        thunk.cache_ref = Some{Any}(value)
    end
    thunk.errored = error
end
function clear_result!(thunk)
    thunk.cache_ref = nothing
    thunk.errored = false
end
function was_scheduled(state, thunk)
    return lock(state.running_state) do running_state
        thunk in running_state.running || thunk.finished
    end
end

"Fills the result for all registered futures of `thunk`."
function fill_registered_futures!(state, thunk, failed)
    futures_to_fill = lock(state.futures) do futures
        if haskey(futures, thunk)
            fs = futures[thunk]
            delete!(futures, thunk)
            return fs
        end
        return nothing
    end
    if futures_to_fill !== nothing
        @dagdebug thunk :finish "Notifying $(length(futures_to_fill)) futures"
        result = load_result(thunk)
        @assert walk_transfer_safe(result) "Result of thunk $(thunk.id) is not transfer safe: $(typeof(result))"
        for future in futures_to_fill
            put!(future, result; error=failed)
        end
    end
end

"""
    register_completion_watcher!(state, thunk_id::TaskID, chan::RemoteChannel)

Registers `chan` to receive a `TaskCompletionNotification` when the task
identified by `thunk_id` completes. If the task has already finished, the
notification is sent immediately.
"""
function register_completion_watcher!(state, thunk_id::TaskID, chan::RemoteChannel)
    already_done = false
    failed = false
    lock(state.completion_watchers) do cw
        if !haskey(state.thunk_dict, thunk_id)
            state.thunk_dict[thunk_id] = WeakThunk(Thunk(()->nothing; id=thunk_id))
        end
        thunk = unwrap_weak_checked(state.thunk_dict[thunk_id])
        if has_result(thunk)
            already_done = true
            failed = has_error(thunk)
            return
        end
        watchers = get!(Vector{RemoteChannel}, cw, thunk)
        push!(watchers, chan)
    end
    if already_done
        try
            thunk = unwrap_weak_checked(state.thunk_dict[thunk_id])
            put!(chan, TaskCompletionNotification(thunk_id, failed, load_result(thunk)))
        catch err
            if !(unwrap_nested_exception(err) isa Union{InvalidStateException, ProcessExitedException})
                rethrow()
            end
        end
    end
end

"""
    notify_completion_watchers!(state, thunk::Thunk, failed::Bool)

Sends a `TaskCompletionNotification` to all channels registered for `thunk`
and removes the registration. Called by `finish_task!` after a task completes.
"""
function notify_completion_watchers!(state, thunk::Thunk, failed::Bool)
    thunk_id = thunk.id
    watchers = lock(state.completion_watchers) do cw
        if haskey(cw, thunk)
            ws = cw[thunk]
            delete!(cw, thunk)
            return ws
        end
        return nothing
    end
    watchers === nothing && return
    @assert walk_transfer_safe(load_result(thunk)) "Result of thunk $thunk_id is not transfer safe: $(typeof(load_result(thunk)))"
    notification = TaskCompletionNotification(thunk_id, failed, load_result(thunk))
    @dagdebug thunk_id :finish "Notifying $(length(watchers)) completion watchers"
    for chan in watchers
        try
            put!(chan, notification)
        catch err
            if !(unwrap_nested_exception(err) isa Union{InvalidStateException, ProcessExitedException})
                rethrow()
            end
        end
    end
end

"Cleans up any syncdeps that aren't needed any longer, and returns a
`Set{Chunk}` of all chunks that can now be evicted from workers."
function cleanup_syncdeps!(state, thunk)
    #to_evict = Set{Chunk}()
    thunk.options.syncdeps === nothing && return
    for inp in thunk.options.syncdeps
        inp = unwrap_weak_checked(inp)
        @assert istask(inp)
        should_cleanup = lock(state.waiting_data) do wd
            if haskey(wd, inp)
                w = wd[inp]
                if thunk in w
                    pop!(w, thunk)
                end
                if isempty(w)
                    delete!(wd, inp)
                    return true
                end
            end
            return false
        end
        if should_cleanup
            inp.sch_accessible = false
            delete_unused_task!(state, inp)
        end
    end
    #return to_evict
end

"Schedules any dependents that may be ready to execute."
function schedule_dependents!(state, thunk, failed)
    @dagdebug thunk :finish "Checking dependents"
    deps_snapshot = lock(state.waiting_data) do wd
        haskey(wd, thunk) || return nothing
        isempty(wd[thunk]) && return nothing
        return collect(wd[thunk])
    end
    deps_snapshot === nothing && return
    ctr = 0
    for dep in deps_snapshot
        dep_isready = lock(state.waiting) do waiting
            if haskey(waiting, dep)
                set = waiting[dep]
                thunk in set && pop!(set, thunk)
                is_ready = isempty(set)
                if is_ready
                    delete!(waiting, dep)
                end
                return is_ready
            else
                return true
            end
        end
        if dep_isready && !was_scheduled(state, dep)
            ctr += 1
            if !failed
                thunk_state = lock(state.thunk_state) do thunk_states
                    thunk_states[dep]
                end
                if try_transition!(thunk_state, THUNK_WAITING, THUNK_READY)
                    push!(state.ready, dep)
                    @dagdebug dep :schedule "Dependent is now ready"
                end
            else
                set_failed!(state, thunk, dep)
                @dagdebug dep :schedule "Dependent has transitively failed"
            end
        end
    end
    @dagdebug thunk :finish "Marked $ctr dependents as $(failed ? "failed" : "ready")"
end

"""
Prepares the scheduler to schedule `thunk`. Will mark `thunk` as ready if
its inputs are satisfied.
"""
function reschedule_syncdeps!(state, thunk, seen=nothing)
    Dagger.maybe_take_or_alloc!(RESCHEDULE_SYNCDEPS_SEEN_CACHE[], seen) do seen
        #=FIXME:REALLOC=#
        to_visit = Thunk[thunk]
        while !isempty(to_visit)
            thunk = pop!(to_visit)
            push!(seen, thunk)
            if haskey(state.valid, thunk)
                continue
            end
            if was_scheduled(state, thunk)
                continue
            end
            for idx in 1:length(thunk.inputs)
                input = Dagger.value(thunk.inputs[idx])
                if input isa WeakChunk
                    input = unwrap_weak_checked(input)
                end
                if input isa Chunk
                    # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                    # so we just pick an equivalent Chunk as our upstream
                    lock(state.waiting_data) do wd
                        push!(get!(()->Set{Thunk}(), wd, input), thunk)
                    end
                end
            end
            w = lock(state.waiting) do waiting
                get!(()->Set{Thunk}(), waiting, thunk)
            end
            if thunk.options.syncdeps !== nothing
                for input in Dagger.syncdeps_iterator(thunk)
                    input in seen && continue

                    # Unseen
                    lock(state.waiting_data) do wd
                        push!(get!(()->Set{Thunk}(), wd, input), thunk)
                    end

                    # Unseen task
                    if has_error(input)
                        set_failed!(state, input, thunk)
                    end
                    input.finished && continue

                    # Unseen and unfinished task
                    lock(state.waiting) do waiting
                        push!(waiting[thunk], input)
                    end
                    if !was_scheduled(state, input)
                        push!(to_visit, input)
                    end
                end
            end
            lock(state.waiting) do waiting
                if haskey(waiting, thunk) && isempty(waiting[thunk])
                    delete!(waiting, thunk)
                end
                if !haskey(waiting, thunk) && !was_scheduled(state, thunk)
                    thunk_state = lock(state.thunk_state) do thunk_states
                        thunk_states[thunk]
                    end
                    if try_transition!(thunk_state, THUNK_WAITING, THUNK_READY)
                        push!(state.ready, thunk)
                    end
                end
            end
        end
    end
end
# N.B. Vector is faster than Set for small collections (which are probably most common)
const RESCHEDULE_SYNCDEPS_SEEN_CACHE = TaskLocalValue{ReusableCache{Set{Thunk},Nothing}}(()->ReusableCache(Set{Thunk}, nothing, 1))

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin::Thunk, thunk::Thunk=origin; ex=nothing)
    has_result(thunk) && return
    if origin === thunk && ex !== nothing
        store_result!(thunk, ex; error=true)
    end
    @lock state.lock begin
        @dagdebug thunk :finish "Setting as failed"

        seen = Set{Thunk}()
        to_visit = Thunk[thunk]
        while !isempty(to_visit)
            thunk = pop!(to_visit)
            push!(seen, thunk)

            filter!(x -> x !== thunk, state.ready)

            if !has_result(thunk) && origin !== thunk
                origin_ex = load_result(origin)
                if origin_ex isa RemoteException
                    origin_ex = origin_ex.captured
                end
                if origin_ex isa DTaskFailedException
                    origin_ex = origin_ex.ex
                end
                ex = DTaskFailedException(thunk, origin, origin_ex)
                store_result!(thunk, ex; error=true)
            end

            fill_registered_futures!(state, thunk, true)
            lock(state.waiting_data) do wd
                if haskey(wd, thunk)
                    for dep in wd[thunk]
                        has_result(dep) && continue
                        dep in seen && continue
                        push!(to_visit, dep)
                    end
                    delete!(wd, thunk)
                end
            end
            thunk.sch_accessible = false
            delete_unused_task!(state, thunk)
            lock(state.waiting) do waiting
                if haskey(waiting, thunk)
                    delete!(waiting, thunk)
                end
            end
        end
    end
end

"Internal utility, useful for debugging scheduler state."
function print_sch_status(state, thunk; kwargs...)
    iob = IOBuffer()
    print_sch_status(iob, state, thunk; kwargs...)
    seek(iob, 0)
    write(stderr, iob)
end
function print_sch_status(io::IO, state, thunk; offset=0, limit=5, max_inputs=3)
    function status_string(thunk)
        status = ""
        if has_error(thunk)
            status *= "E"
        end
        running = lock(state.running_state) do running_state
            thunk in running_state.running
        end
        if has_result(thunk)
            status *= "C"
        elseif thunk in state.ready
            status *= "r"
        elseif running
            status *= "R"
        else
            status *= "?"
        end
        status
    end

    # General scheduler status
    if offset == 0
        thunk_states = lock(copy, state.thunk_state)
        println(io, "Ready ($(length(state.ready))): $(join(map(t->t.id, state.ready), ','))")
        lock(state.running_state) do running_state
            println(io, "Running: ($(length(running_state.running))): $(join(map(t->"$(t.id)($(thunk_states[t].value)[$(status_string(t))])", collect(running_state.running)), ", "))")
        end
        wtp = state.worker_time_pressure[myid()]
        lock(wtp) do wtp
            for proc in keys(wtp)
                println(io, "  Sch Processor: $(proc) time pressure $(wtp[proc])")
            end
        end
    end

    # Processors status
    if offset == 0
        states = proc_states_values(state.uid)
        for state in states
            istate = state.state
            println(io, "Processor: $(istate.proc):")
            lock(istate.queue) do queue
                println(io, "  Occupancy $(istate.proc_occupancy[])")
                println(io, "  Time Pressure $(istate.time_pressure[])")
                println(io, "  Running $(length(istate.tasks)): $(join(collect(keys(istate.tasks)), ", "))")
                println(io, "  Queue $(length(queue)):")
                for task in queue
                    println(io, "    $(task.thunk_id): $(task.est_time_util) $(task.est_occupancy)")
                end
            end
        end
    end

    # Thunk status
    if offset == 0
        print(io, "($(status_string(thunk))) ")
    end
    println(io, "$(thunk.id): $(thunk.f)")
    for (idx, input) in enumerate(thunk.options.syncdeps)
        if input isa WeakThunk
            input = Dagger.unwrap_weak(input)
            if input === nothing
                println(io, repeat(' ', offset+2), "(???)")
                continue
            end
        end
        input isa Thunk || continue
        if idx > max_inputs
            println(io, repeat(' ', offset+2), "…")
            break
        end
        status = status_string(input)
        if haskey(state.waiting, thunk) && input in state.waiting[thunk]
            status *= "W"
        end
        if haskey(state.waiting_data, input) && thunk in state.waiting_data[input]
            status *= "w"
        end
        if haskey(state.futures, input)
            status *= "f($(length(state.futures[input])))"
        end
        print(io, repeat(' ', offset+2), "($status) ")
        if limit > 0
            print_sch_status(io, state, input; offset=offset+2, limit=limit-1)
        else
            println(io, "…")
        end
    end
end

function fetch_report(task)
    try
        fetch(task)
    catch err
        @static if VERSION < v"1.7-rc1"
            stk = Base.catch_stack(task)
        else
            stk = Base.current_exceptions(task)
        end
        err, frames = stk[1]
        rethrow(CapturedException(err, frames))
    end
end

function report_catch_error(err, desc=nothing)
    iob = IOContext(IOBuffer(), :color=>true)
    if desc !== nothing
        println(iob, desc)
    end
    Base.showerror(iob, err)
    Base.show_backtrace(iob, catch_backtrace())
    println(iob)
    seek(iob.io, 0)
    write(stderr, iob)
end

chunktype(x) = typeof(x)
signature(state, task::Thunk) =
    signature(task.inputs[1], @view task.inputs[2:end])
function signature(f, args)
    n_pos = count(Dagger.ispositional, args)
    any_kw = any(!Dagger.ispositional, args)
    kw_extra = any_kw ? 2 : 0
    sig = Vector{Any}(undef, 1+n_pos+kw_extra)
    sig[1+kw_extra] = chunktype(f)
    #=FIXME:REALLOC_N=#
    sig_kwarg_names = Symbol[]
    sig_kwarg_types = []
    for idx in 1:length(args)
        arg = args[idx]
        value = Dagger.value(arg)
        if value isa Dagger.DTask
            # Only occurs via manual usage of signature
            value = fetch(value; raw=true)
        end
        if istask(value)
            throw(ConcurrencyViolationError("Must call `collect_task_inputs!(state, task)` before calling `signature`"))
        end
        T = chunktype(value)
        if Dagger.ispositional(arg)
            sig[1+idx+kw_extra] = T
        else
            push!(sig_kwarg_names, Dagger.pos_kw(arg))
            push!(sig_kwarg_types, T)
        end
    end
    if any_kw
        NT = NamedTuple{(sig_kwarg_names...,), Base.to_tuple_type(sig_kwarg_types)}
        sig[2] = NT
        @static if isdefined(Core, :kwcall)
            sig[1] = typeof(Core.kwcall)
        else
            f_instance = chunktype(f).instance
            kw_f = Core.kwfunc(f_instance)
            sig[1] = typeof(kw_f)
        end
    end
    #=FIXME:UNIQUE=#
    return Signature(sig)
end

function can_use_proc(state, task, gproc, proc, opts, scope)
    # Check against proclist
    if opts.proclist !== nothing
        @warn "The `proclist` option is deprecated, please use scopes instead\nSee https://juliaparallel.org/Dagger.jl/stable/scopes/ for details" maxlog=1
        if opts.proclist isa Function
            if !Base.invokelatest(opts.proclist, proc)
                @dagdebug task :scope "Rejected $proc: proclist(proc) == false"
                return false, scope
            end
            scope = constrain(scope, Dagger.ExactScope(proc))
        elseif opts.proclist isa Vector
            if !(typeof(proc) in opts.proclist)
                @dagdebug task :scope "Rejected $proc: !(typeof(proc) in proclist) ($(opts.proclist))"
                return false, scope
            end
            scope = constrain(scope,
                              Dagger.UnionScope(map(Dagger.ProcessorTypeScope, opts.proclist)))
        else
            throw(SchedulingException("proclist must be a Function, Vector, or nothing"))
        end
        if scope isa Dagger.InvalidScope
            @dagdebug task :scope "Rejected $proc: Not contained in task scope ($scope)"
            return false, scope
        end
    end

    # Check against single
    if opts.single !== nothing
        @warn "The `single` option is deprecated, please use scopes instead\nSee https://juliaparallel.org/Dagger.jl/stable/scopes/ for details" maxlog=1
        if gproc.pid != opts.single
            @dagdebug task :scope "Rejected $proc: gproc.pid ($(gproc.pid)) != single ($(opts.single))"
            return false, scope
        end
        scope = constrain(scope, Dagger.ProcessScope(opts.single))
        if scope isa Dagger.InvalidScope
            @dagdebug task :scope "Rejected $proc: Not contained in task scope ($scope)"
            return false, scope
        end
    end

    # Check against scope
    if !Dagger.proc_in_scope(proc, scope)
        @dagdebug task :scope "Rejected $proc: Not contained in task scope ($scope)"
        return false, scope
    end

    # Check against function and arguments
    Tf = chunktype(task.f)
    if !Dagger.iscompatible_func(proc, opts, Tf)
        @dagdebug task :scope "Rejected $proc: Not compatible with function type ($Tf)"
        return false, scope
    end
    for arg in task.inputs[2:end]
        value = unwrap_weak_checked(Dagger.value(arg))
        if value isa Thunk
            value = load_result(value)
        end
        Targ = chunktype(value)
        if !Dagger.iscompatible_arg(proc, opts, Targ)
            @dagdebug task :scope "Rejected $proc: Not compatible with argument type ($Targ)"
            return false, scope
        end
    end

    @label accept

    @dagdebug task :scope "Accepted $proc"
    return true, scope
end

function has_capacity(state, p, gp, time_util, alloc_util, occupancy, sig)
    T = typeof(p)
    # FIXME: MaxUtilization
    est_time_util = round(UInt64, if time_util !== nothing && haskey(time_util, T)
        time_util[T] * 1000^3
    else
        get(state.signature_time_cost, sig, 1000^3)
    end)::UInt64
    est_alloc_util = if alloc_util !== nothing && haskey(alloc_util, T)
        alloc_util[T]
    else
        get(state.signature_alloc_cost, sig, UInt64(0))
    end::UInt64
    est_occupancy::UInt32 = typemax(UInt32)
    if occupancy !== nothing
        occ = nothing
        if haskey(occupancy, T)
            occ = occupancy[T]
        elseif haskey(occupancy, Any)
            occ = occupancy[Any]
        end
        if occ !== nothing
            # Clamp to 0-1, and scale between 0 and `typemax(UInt32)`
            est_occupancy = Base.unsafe_trunc(UInt32, clamp(occ, 0, 1) * typemax(UInt32))
        end
    end
    #= FIXME: Estimate if cached data can be swapped to storage
    storage = storage_resource(p)
    real_alloc_util = state.worker_storage_pressure[gp][storage]
    real_alloc_cap = state.worker_storage_capacity[gp][storage]
    if est_alloc_util + real_alloc_util > real_alloc_cap
        return false, est_time_util, est_alloc_util
    end
    =#
    return true, est_time_util, est_alloc_util, est_occupancy
end

"Like `sum`, but replaces `nothing` entries with the average of non-`nothing` entries."
function impute_sum(xs)
    total = 0
    nothing_count = 0
    something_count = 0
    for x in xs
        if isnothing(x)
            nothing_count += 1
        else
            something_count += 1
            total += x
        end
    end

    something_count == 0 && return 0
    return total + nothing_count * total / something_count
end

"Collects all arguments for `task`, converting Thunk inputs to Chunks."
collect_task_inputs!(state, task::Thunk) =
    collect_task_inputs!(state, task.inputs)
function collect_task_inputs!(state, inputs)
    for idx in 1:length(inputs)
        input = unwrap_weak_checked(Dagger.value(inputs[idx]))
        if istask(input)
            inputs[idx].value = wrap_weak(load_result(input))
        end
    end
    return
end

"""
Estimates the cost of scheduling `task` on each processor in `procs`. Considers
current estimated per-processor compute pressure, and transfer costs for each
`Chunk` argument to `task`. Returns `(procs, costs)`, with `procs` sorted in
order of ascending cost.
"""
function estimate_task_costs(state, procs, task; sig=nothing)
    sorted_procs = Vector{Processor}(undef, length(procs))
    costs = Dict{Processor,Float64}()
    estimate_task_costs!(sorted_procs, costs, state, procs, task; sig)
    return sorted_procs, costs
end
@reuse_scope function estimate_task_costs!(sorted_procs, costs, state, procs, task; sig=nothing)
    tx_rate = state.transfer_rate[]

    # Find all Chunks
    chunks = @reusable_vector :estimate_task_costs_chunks Union{Chunk,Nothing} nothing 32
    chunks_cleanup = @reuse_defer_cleanup empty!(chunks)
    for input in task.inputs
        if Dagger.valuetype(input) <: Chunk
            push!(chunks, Dagger.value(input)::Chunk)
        end
    end

    # Estimate the cost of executing the task itself
    if sig === nothing
        sig = signature(task.f, task.inputs)
    end
    est_time_util = get(state.signature_time_cost, sig, 1000^3)

    # Estimate total cost for executing this task on each candidate processor
    for proc in procs
        gproc = get_parent(proc)
        chunks_filt = Iterators.filter(c->get_parent(processor(c)) != gproc, chunks)

        # Estimate network transfer costs based on data size
        # N.B. `affinity(x)` really means "data size of `x`"
        # N.B. We treat same-worker transfers as having zero transfer cost
        # TODO: For non-Chunk, model cost from scheduler to worker
        # TODO: Measure and model processor move overhead
        tx_cost = impute_sum(affinity(chunk)[2] for chunk in chunks_filt)

        # Add fixed cost for cross-worker task transfer (esimated at 1ms)
        # TODO: Actually estimate/benchmark this
        task_xfer_cost = gproc.pid != myid() ? 1_000_000 : 0 # 1ms

        # Compute final cost
        costs[proc] = est_time_util + (tx_cost/tx_rate) + task_xfer_cost
    end
    chunks_cleanup()

    # Shuffle procs around, so equally-costly procs are equally considered
    np = length(procs)
    @reusable :estimate_task_costs_P Vector{Int} 0 4 np P begin
        resize!(P, np)
        copyto!(P, 1:np)
        randperm!(P)
        for idx in 1:np
            sorted_procs[idx] = procs[P[idx]]
        end
    end

    # Sort by lowest cost first
    sort!(sorted_procs, by=p->costs[p])
end

"""
    walk_data(f, x)

Walks the data contained in `x` in DFS fashion, and executes `f` at each object
that hasn't yet been seen.
"""
function walk_data(f, @nospecialize(x))
    action = f(x)
    if action !== missing
        return action
    end

    seen = IdDict{Any,Nothing}()
    to_visit = Any[x]

    while !isempty(to_visit)
        y = pop!(to_visit)
        if !walk_data_inner(f, y, seen, to_visit)
            return false
        end
    end

    return true
end
function walk_data_inner(f, x, seen, to_visit)
    if !isstructtype(typeof(x))
        return true
    end
    for field in fieldnames(typeof(x))
        isdefined(x, field) || continue
        next = getfield(x, field)
        if !haskey(seen, next)
            seen[next] = nothing
            action = f(next)
            if action === false
                return false
            elseif action === missing
                push!(to_visit, next)
            end
        end
    end
    return true
end
function walk_data_inner(f, x::Union{Array,Tuple}, seen, to_visit)
    for idx in firstindex(x):lastindex(x)
        if x isa Array
            isassigned(x, idx) || continue
        end
        next = x[idx]
        if !haskey(seen, next)
            seen[next] = nothing
            action = f(next)
            if action === false
                return false
            elseif action === missing
                push!(to_visit, next)
            end
        end
    end
    return true
end
walk_data_inner(f, ::DataType, seen, to_visit) = true

function walk_data_bool(@specialize(f), @nospecialize(x))
    safe = Ref{Bool}(true)
    walk_data(x) do y
        action = f(y)
        if action === false
            safe[] = false
        end
        return action
    end
    return safe[]
end

"Walks `x` and returns a `Bool` indicating whether `x` is safe to store to disk."
walk_storage_safe(@nospecialize(x)) =
    walk_data_bool(storage_safe, x)

storage_safe(::T) where T = storage_safe_type(T)

function storage_safe_type(::Type{T}) where T
    isprimitivetype(T) && return true
    isabstracttype(T) && return missing
    if T isa Union
        for S in Base.uniontypes(T)
            action = storage_safe_type(S)
            if action !== true
                return action
            end
        end
    end
    return true
end
storage_safe_type(::Type{A}) where {A<:Array{T}} where {T} =
    storage_safe_type(T)

storage_safe_type(::Type{Thunk}) = false
storage_safe_type(::Type{Dagger.DTask}) = false
storage_safe_type(::Type{<:Chunk}) = false
storage_safe_type(::Type{MemPool.DRef}) = false
storage_safe_type(::Type{<:Ptr}) = false
storage_safe_type(::Type{<:Core.LLVMPtr}) = false

"Walks `x` and returns a `Bool` indicating whether `x` is safe to transfer."
walk_transfer_safe(@nospecialize(x)) =
    walk_data_bool(transfer_safe, x)

transfer_safe(::T) where T = transfer_safe_type(T)

function transfer_safe_type(::Type{T}) where T
    isprimitivetype(T) && return true
    isabstracttype(T) && return missing
    if T isa Union
        for S in Base.uniontypes(T)
            action = transfer_safe_type(S)
            if action !== true
                return action
            end
        end
    end
    return true
end
transfer_safe_type(::Type{A}) where {A<:Array{T}} where {T} =
    transfer_safe_type(T)

transfer_safe_type(::Type{Thunk}) = false
transfer_safe_type(::Type{Dagger.DTask}) = false
transfer_safe_type(::Type{<:Chunk}) = true
transfer_safe_type(::Type{MemPool.DRef}) = true
transfer_safe_type(::Type{<:Ptr}) = false
transfer_safe_type(::Type{<:Core.LLVMPtr}) = false
