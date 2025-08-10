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
    @assert thunk.finished "Thunk[$(thunk.id)] is not yet finished"
    return something(thunk.cache_ref)
end
function store_result!(state, thunk, value; error::Bool=false)
    @assert islocked(state.lock)
    @assert !thunk.finished "Thunk[$(thunk.id)] should not be finished yet"
    @assert !has_result(state, thunk) "Thunk[$(thunk.id)] already contains a cached result"
    thunk.finished = true
    if error && value isa Exception && !(value isa DTaskFailedException)
        thunk.cache_ref = Some{Any}(DTaskFailedException(thunk, thunk, value))
    else
        thunk.cache_ref = Some{Any}(value)
    end
    state.errored[thunk] = error
end
function clear_result!(state, thunk)
    @assert islocked(state.lock)
    thunk.cache_ref = nothing
    delete!(state.errored, thunk)
end

"Fills the result for all registered futures of `thunk`."
function fill_registered_futures!(state, thunk, failed)
    if haskey(state.futures, thunk)
        # Notify any listening thunks
        @dagdebug thunk :finish "Notifying $(length(state.futures[thunk])) futures"
        for future in state.futures[thunk]
            put!(future, load_result(state, thunk); error=failed)
        end
        delete!(state.futures, thunk)
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
        if inp in keys(state.waiting_data)
            w = state.waiting_data[inp]
            if thunk in w
                pop!(w, thunk)
            end
            if isempty(w)
                #= FIXME: Worker-side cache is currently disabled
                if istask(inp) && has_result(state, inp)
                    _thunk = load_result(state, inp)
                    if _thunk isa Chunk
                        push!(to_evict, _thunk)
                    end
                elseif inp isa Chunk
                    push!(to_evict, inp)
                end
                =#
                delete!(state.waiting_data, inp)
                inp.sch_accessible = false
                delete_unused_task!(state, inp)
            end
        end
    end
    #return to_evict
end

"Schedules any dependents that may be ready to execute."
function schedule_dependents!(state, thunk, failed)
    @dagdebug thunk :finish "Checking dependents"
    if !haskey(state.waiting_data, thunk) || isempty(state.waiting_data[thunk])
        return
    end
    ctr = 0
    for dep in state.waiting_data[thunk]
        @dagdebug dep :schedule "Checking dependent"
        dep_isready = false
        if haskey(state.waiting, dep)
            set = state.waiting[dep]
            thunk in set && pop!(set, thunk)
            if length(set) > 0
                @dagdebug dep :schedule "Dependent has $(length(set)) upstreams"
            end
            dep_isready = isempty(set)
            if dep_isready
                delete!(state.waiting, dep)
            end
        else
            dep_isready = true
        end
        if dep_isready
            ctr += 1
            if !failed
                push!(state.ready, dep)
                @dagdebug dep :schedule "Dependent is now ready"
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
            if thunk.finished || (thunk in state.ready) || (thunk in state.running)
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
                    if !haskey(state.waiting_data, input)
                        push!(get!(()->Set{Thunk}(), state.waiting_data, input), thunk)
                    end
                end
            end
            w = get!(()->Set{Thunk}(), state.waiting, thunk)
            if thunk.options.syncdeps !== nothing
                syncdeps = WeakThunk[thunk.options.syncdeps...]
                for weak_input in syncdeps
                    input = unwrap_weak_checked(weak_input)::Thunk
                    input in seen && continue

                    # Unseen
                    push!(get!(()->Set{Thunk}(), state.waiting_data, input), thunk)
                    #istask(input) || continue

                    # Unseen task
                    if get(state.errored, input, false)
                        set_failed!(state, input, thunk)
                    end
                    input.finished && continue

                    # Unseen and unfinished task
                    push!(w, input)
                    if !((input in state.running) || (input in state.ready))
                        push!(to_visit, input)
                    end
                end
            end
            if isempty(w)
                # Inputs are ready
                delete!(state.waiting, thunk)
                if !get(state.errored, thunk, false)
                    push!(state.ready, thunk)
                end
            end
        end
    end
end
# N.B. Vector is faster than Set for small collections (which are probably most common)
const RESCHEDULE_SYNCDEPS_SEEN_CACHE = TaskLocalValue{ReusableCache{Set{Thunk},Nothing}}(()->ReusableCache(Set{Thunk}, nothing, 1))

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin, thunk=origin)
    @assert islocked(state.lock)
    has_result(state, thunk) && return
    @dagdebug thunk :finish "Setting as failed"
    filter!(x -> x !== thunk, state.ready)
    # N.B. If origin === thunk, we assume that the caller has already set the error
    if origin !== thunk
        origin_ex = load_result(state, origin)
        if origin_ex isa RemoteException
            origin_ex = origin_ex.captured
        end
        ex = DTaskFailedException(thunk, origin, origin_ex)
        store_result!(state, thunk, ex; error=true)
    end
    finish_failed!(state, thunk, origin)
end
function finish_failed!(state, thunk, origin=nothing)
    @assert islocked(state.lock)
    fill_registered_futures!(state, thunk, true)
    if haskey(state.waiting_data, thunk)
        for dep in state.waiting_data[thunk]
            haskey(state.waiting, dep) &&
                delete!(state.waiting, dep)
            haskey(state.errored, dep) &&
                continue
            origin !== nothing && set_failed!(state, origin, dep)
        end
        delete!(state.waiting_data, thunk)
        thunk.sch_accessible = false
        delete_unused_task!(state, thunk)
    end
    if haskey(state.waiting, thunk)
        delete!(state.waiting, thunk)
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
    function status_string(node)
        status = ""
        if get(state.errored, node, false)
            status *= "E"
        end
        if node in state.ready
            status *= "r"
        elseif node in state.running
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
        println(io, "Running: ($(length(state.running))): $(join(map(t->t.id, collect(state.running)), ','))")
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
