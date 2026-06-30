"Like `errormonitor`, but tracks how many outstanding tasks are running."
function errormonitor_tracked(name::String, t::Task)
    errormonitor(t)
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
    unwrap_nested_exception(err.t.exception)
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

has_result(state, thunk) = thunk.cache_ref !== nothing
function load_result(state, thunk)
    @assert (@atomic thunk.finished) "Thunk[$(thunk.id)] is not yet finished"
    return something(thunk.cache_ref)
end
"""
    store_result!(state, thunk, value; error=false)

Store `value` as the result of `thunk` and mark it finished. Uses a
single-finisher CAS on `thunk.finished` so that duplicate or racing
completions (cancellation, worker fault) are silently dropped — only the
first caller proceeds. Returns `true` if this call won the race, `false`
if another finisher already stored a result.
"""
function store_result!(state, thunk, value; error::Bool=false)
    # CAS: exactly one finisher proceeds. `@atomicreplace` returns (old, success).
    _, won = @atomicreplace thunk.finished false => true
    won || return false
    if error && value isa Exception && !(value isa DTaskFailedException)
        thunk.cache_ref = Some{Any}(DTaskFailedException(thunk, thunk, value))
    else
        thunk.cache_ref = Some{Any}(value)
    end
    @atomic thunk.errored = error
    return true
end
function clear_result!(state, thunk)
    @assert islocked(state.lock)
    thunk.cache_ref = nothing
    @atomic thunk.finished = false
    @atomic thunk.errored = false
end

"Seals the futures Treiber list on `thunk` and fulfills all registered futures with its result."
function fill_registered_futures!(state, thunk, failed)
    head = futures_seal!(thunk)
    head === nothing && return
    result = load_result(state, thunk)
    @dagdebug thunk :finish "Notifying futures"
    node = head
    while node !== nothing
        put!(node.future, result; error=failed)
        node = @atomic node.next
    end
end


"""
Seal the dependents Treiber list on `thunk` and, for each captured downstream:
- Atomically decrement its `pending_deps` counter.
- If the counter just hit zero and `failed` is false, push it to `state.ready`.
- If `failed` is true or the dependent is already errored, propagate failure
  immediately (regardless of how many other upstreams remain).
"""
function schedule_dependents!(state, thunk, failed)
    @dagdebug thunk :finish "Checking dependents"
    head = deps_seal!(thunk)
    head === nothing && return
    ctr = 0
    node = head
    while node !== nothing
        dep = node.thunk::Thunk
        n = @atomic dep.pending_deps -= 1   # n = new value after decrement
        # Dataflow invariant check: the counter must never go negative.
        # A negative value means more decrements than increments were issued,
        # which would indicate a bug in the submission-guard protocol.
        @assert n >= 0 "BUG: pending_deps underflow on Thunk[$(dep.id)]: n=$n after decrement by $(thunk.id)"
        if failed || (@atomic dep.errored)
            # Propagate failure immediately — don't wait for the counter to
            # reach zero (mirrors the old DFS semantics in set_failed!).
            ctr += 1
            @dagdebug dep :schedule "Dependent has transitively failed"
            (@atomic dep.finished) || set_failed!(state, thunk, dep)
        elseif n == 0
            # This was the last pending upstream; the dependent is now ready.
            ctr += 1
            @dagdebug dep :schedule "Dependent is now ready"
            # Invariant: if we reached n==0 and are about to push to
            # ready, the counter should still be 0 (not driven negative by
            # another concurrent decrement, which would be a protocol error).
            n_now = @atomic dep.pending_deps
            @assert n_now == 0 "Thunk[$(dep.id)] pending_deps drifted from 0 to $n_now between ready decision and push"
            (@atomic dep.finished) || push!(state.ready, dep)
        end
        node = @atomic node.next
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
            cur = pop!(to_visit)
            push!(seen, cur)

            # Skip thunks that are already registered, running, or done.
            if (@atomic cur.valid)
                continue
            end
            if (@atomic cur.finished) || (cur in state.ready) || (@atomic cur.running)
                continue
            end

            # Submission-guard: hold pending_deps at ≥1 while we wire edges
            # so that no upstream can drive it to zero prematurely.
            @atomic cur.pending_deps = 1

            errored_input = nothing
            if cur.options !== nothing && cur.options.syncdeps !== nothing
                for input in Dagger.syncdeps_iterator(cur)
                    if (@atomic input.errored)
                        # Record the first errored upstream; process remaining
                        # edges so we don't skip incrementing for others (they
                        # will be undone by set_failed! if needed).
                        errored_input = input
                        continue
                    end

                    if !(@atomic input.finished)
                        # Register cur as a downstream dependent of input.
                        @atomic cur.pending_deps += 1
                        pushed = deps_push!(input, cur)
                        if !pushed
                            # input finished between our check and the push;
                            # undo the +1 (the seal-swap already happened).
                            @atomic cur.pending_deps -= 1
                        end

                        # DFS into input only if we haven't visited it yet and
                        # it is not already being scheduled elsewhere.
                        if !(input in seen) &&
                                !((@atomic input.running) || (input in state.ready))
                            push!(to_visit, input)
                        end
                    end
                    # Finished (non-errored) input contributes no pending dep.
                end
            end

            # Release the guard reference.
            n = @atomic cur.pending_deps -= 1   # n = new value after decrement

            if errored_input !== nothing
                # At least one upstream was already errored: fail this thunk.
                # set_failed! calls schedule_dependents!(cur, true) which seals
                # cur.dependents_head, so the ready-push below is skipped.
                set_failed!(state, errored_input, cur)
            elseif n == 0 && !(@atomic cur.errored) && !(@atomic cur.finished)
                # All upstream edges satisfied and no error: cur is ready.
                push!(state.ready, cur)
            end
        end
    end
end
# N.B. Vector is faster than Set for small collections (which are probably most common)
const RESCHEDULE_SYNCDEPS_SEEN_CACHE = TaskLocalValue{ReusableCache{Set{Thunk},Nothing}}(()->ReusableCache(Set{Thunk}, nothing, 1))

"Marks `thunk` (and all transitive dependents) as failed, then propagates."
function set_failed!(state, origin::Thunk, thunk::Thunk=origin; ex=nothing)
    @assert islocked(state.lock)
    has_result(state, thunk) && return
    @dagdebug thunk :finish "Setting as failed"

    if origin === thunk && ex !== nothing
        store_result!(state, thunk, ex; error=true)
    elseif !has_result(state, thunk)
        origin_ex = load_result(state, origin)
        if origin_ex isa RemoteException
            origin_ex = origin_ex.captured
        end
        if origin_ex isa DTaskFailedException
            origin_ex = origin_ex.ex
        end
        store_result!(state, thunk, DTaskFailedException(thunk, origin, origin_ex); error=true)
    end

    filter!(x -> x !== thunk, state.ready)
    fill_registered_futures!(state, thunk, true)
    thunk.sch_accessible = false
    delete_unused_task!(state, thunk)

    # Seal the dependents list and propagate failure transitively.
    # schedule_dependents! with failed=true immediately calls set_failed! for
    # each captured dependent, regardless of their pending_deps counter value,
    # mirroring the DFS semantics of the old waiting_data traversal.
    schedule_dependents!(state, thunk, true)
end

"Internal utility, useful for debugging scheduler state."
function print_sch_status(state, thunk; kwargs...)
    iob = IOBuffer()
    print_sch_status(iob, state, thunk; kwargs...)
    seek(iob, 0)
    write(stderr, iob)
end
function print_sch_status(io::IO, state, thunk; offset=0, limit=5, max_inputs=3)
    function status_string(node)
        status = ""
        if (@atomic node.errored)
            status *= "E"
        end
        if node in state.ready
            status *= "r"
        elseif (@atomic node.running)
            status *= "R"
        elseif has_result(state, node)
            status *= "C"
        else
            status *= "?"
        end
        status
    end
    if offset == 0
        println(io, "Ready ($(length(state.ready))): $(join(map(t->t.id, state.ready), ','))")
        println(io, "Running ($(state.running_count[])): (use thunk.running to inspect individual tasks)")
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
        let pd = @atomic thunk.pending_deps
            pd > 0 && (status *= "W")  # thunk still has unresolved upstreams
        end
        let dh = @atomic input.dependents_head
            if !(dh isa Sealed) && dh !== nothing
                status *= "w"  # input has registered (not-yet-notified) dependents
            end
        end
        let fh = @atomic input.futures_head
            if !(fh isa Sealed) && fh !== nothing
                status *= "f(?)"  # Treiber list — count not tracked; just signal presence
            end
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
            value = load_result(state, value)
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
        lock(state.signature_time_cost) do stc; get(stc, sig, 1000^3); end
    end)::UInt64
    est_alloc_util = if alloc_util !== nothing && haskey(alloc_util, T)
        alloc_util[T]
    else
        lock(state.signature_alloc_cost) do sac; get(sac, sig, UInt64(0)); end
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
            inputs[idx].value = wrap_weak(load_result(state, input))
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
const DEFAULT_TRANSFER_RATE = UInt64(1_000_000)
@reuse_scope function estimate_task_costs!(sorted_procs, costs, state, procs, task; sig=nothing)

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
    est_time_util = lock(state.signature_time_cost) do stc; get(stc, sig, 1000^3); end

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

        tx_rate = lock(state.worker_transfer_rate) do wtr
            get(get(wtr, gproc.pid, Dict{Processor,UInt64}()), proc, DEFAULT_TRANSFER_RATE)
        end
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

"Walks `x` and returns a `Bool` indicating whether `x` is safe to serialize."
function walk_storage_safe(@nospecialize(x))
    safe = Ref{Bool}(true)
    walk_data(x) do y
        action = storage_safe(y)
        if action === false
            safe[] = false
        end
        return action
    end
    safe[]
end

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
