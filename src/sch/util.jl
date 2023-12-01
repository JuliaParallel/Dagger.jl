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
const ERRORMONITOR_TRACKED = LockedObject(Pair{String,Task}[])

"""
    unwrap_nested_exception(err::Exception) -> Bool

Extracts the "core" exception from a nested exception."
"""
unwrap_nested_exception(err::CapturedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::RemoteException) =
    unwrap_nested_exception(err.captured)
unwrap_nested_exception(err) = err

"Gets a `NamedTuple` of options propagated by `thunk`."
function get_propagated_options(options, thunk=nothing)
    options::ThunkOptions
    nt = NamedTuple()
    if options.propagates === nothing
        return nt
    end
    for key in options.propagates
        value = if key == :scope
            isa(thunk.f, Chunk) ? thunk.f.scope : DefaultScope()
        elseif key == :processor
            isa(thunk.f, Chunk) ? thunk.f.processor : OSProc()
        elseif key in fieldnames(Thunk)
            getproperty(thunk, key)
        elseif key in fieldnames(ThunkOptions)
            getproperty(thunk.options, key)
        else
            throw(ArgumentError("Can't propagate unknown key: $key"))
        end
        nt = merge(nt, (key=>value,))
    end
    return nt
end

""""
Returns the result and error status of `thunk` if it's cached in the scheduler,
as a `Some{Tuple{<:Any,Bool}}`. If it's not present, `nothing` is returned.
"""
function cache_lookup(state, thunk::Thunk)
    if haskey(state.cache, thunk)
        return Some((state.cache[thunk], get(state.errored, thunk, false)))
    end
    return nothing
end
function cache_lookup(state, thunk::ThunkRef)
    if haskey(state.cache_remote, thunk)
        return Some((state.cache_remote[thunk], get(state.errored_remote, thunk, false)))
    end
    return nothing
end
function cache_lookup_checked(state, thunk)
    value = cache_lookup(state, thunk)
    if value === nothing
        throw(KeyError(thunk))
    end
    return value.value
end

"Stores `value` and `error` as the cached value and error status of `thunk`."
function cache_store!(state, thunk::Thunk, value, error=false)
    state.cache[thunk] = value
    state.errored[thunk] = error
end
function cache_store!(state, thunk::ThunkRef, value, error=false)
    state.cache_remote[thunk] = value
    state.errored_remote[thunk] = error
end

"""
Removes `thunk` from the preservation set of `thunk_ref`; if
`thunk_ref`'s preservation set becomes empty, removes the cached result of
`thunk_ref`.
"""
function cache_evict_remote!(state, thunk_ref::ThunkRef, thunk::Thunk)
    pres_set = state.waiting_remote[thunk_ref]
    pop!(pres_set, thunk)
    if length(pres_set) == 0
        delete!(state.waiting_remote, thunk_ref)
        delete!(state.cache_remote, thunk_ref)
        delete!(state.errored_remote, thunk_ref)
    end
end

"Fills the result for all registered futures of `node`."
function fill_registered_futures!(state, node, failed)
    if haskey(state.futures, node)
        # Notify any listening thunks
        for future in state.futures[node]
            put!(future, state.cache[node]; error=failed)
        end
        delete!(state.futures, node)
    end
end

"""
Cleans up any syncdeps that aren't needed any longer, and returns a
`Set{Chunk}` of all chunks that can now be evicted from workers.
"""
function cleanup_syncdeps!(state, node)
    to_evict = Set{Chunk}()
    for inp in node.syncdeps
        inp = unwrap_weak_checked(inp)
        if !istask(inp) && !(inp isa Chunk)
            continue
        end
        if inp in keys(state.waiting_data)
            w = state.waiting_data[inp]
            if node in w
                pop!(w, node)
            end
            if isempty(w)
                if istask(inp)
                    value = cache_lookup(state, inp)
                    if value !== nothing
                        value, _ = value.value
                        if value isa Chunk
                            push!(to_evict, value)
                        end
                        if inp isa ThunkRef
                            # We're done executing, remove us from the preservation set
                            cache_evict_remote!(state, inp, node)
                        end
                    end
                elseif inp isa Chunk
                    push!(to_evict, inp)
                end
                delete!(state.waiting_data, inp)
            end
        end
    end
    return to_evict
end

"Schedules any dependents that may be ready to execute."
function schedule_dependents!(state, node, failed)
    for dep in sort!(collect(get(()->Set{AnyThunk}(), state.waiting_data, node)), by=state.node_order)
        # If remote dep, we will notify the owning scheduler in `fill_registered_futures!`
        dep isa ThunkRef && continue

        # Is this dependent ready to execute?
        dep_isready = false
        if haskey(state.waiting, dep)
            set = state.waiting[dep]
            node in set && pop!(set, node)
            dep_isready = isempty(set)
            if dep_isready
                delete!(state.waiting, dep)
            end
        else
            dep_isready = true
        end
        if dep_isready
            if !failed
                push!(state.ready, dep)
            end
        end
    end
end

"Preserves local thunk dependents of the remote `thunk`."
function preserve_local_dependents!(state, thunk::ThunkRef)
    pres_set = get!(()->Set{Thunk}(), state.waiting_remote, thunk)
    for dep in get(()->Set{AnyThunk}(), state.waiting_data, thunk)
        push!(pres_set, dep)
    end
end

"""
Prepares the scheduler to schedule `thunk`. Will mark `thunk` as ready if
its inputs are satisfied.
"""
function reschedule_syncdeps!(state, thunk, seen=Set{AnyThunk}())
    to_visit = AnyThunk[thunk]
    while !isempty(to_visit)
        thunk = pop!(to_visit)
        push!(seen, thunk)
        if haskey(state.valid, thunk)
            continue
        end
        if haskey(state.cache, thunk) || (thunk in state.ready) || (thunk in state.running)
            continue
        end
        if thunk isa ThunkRef
            # We don't own these, so stop here
            continue
        end

        for (_, input) in thunk.inputs
            if input isa WeakChunk
                input = unwrap_weak_checked(input)
            end
            if input isa Chunk
                # N.B. Different Chunks with the same DRef handle will hash to the same slot,
                # so we just pick an equivalent Chunk as our upstream
                if !haskey(state.waiting_data, input)
                    push!(get!(()->Set{AnyThunk}(), state.waiting_data, input), thunk)
                end
            end
        end
        w = get!(()->Set{AnyThunk}(), state.waiting, thunk)
        for input in thunk.syncdeps
            input = unwrap_weak_checked(input)
            istask(input) && input in seen && continue

            # Unseen
            push!(get!(()->Set{AnyThunk}(), state.waiting_data, input), thunk)
            istask(input) || continue

            # Unseen task
            result_err = cache_lookup(state, input)
            if result_err !== nothing
                if something(result_err)[2]
                    set_failed!(state, input, thunk)
                else
                    continue
                end
            end

            # Unseen and unfinished task
            push!(w, input)
            if input isa Thunk
                if !((input in state.running) || (input in state.ready))
                    push!(to_visit, input)
                end
            elseif input isa ThunkRef
                # Remote thunk, ask to be notified when it's completed
                # Register a future with the owning scheduler
                register_remote_future!(state, input)
            end
        end
        if isempty(w)
            # Inputs are satisfied, so we're either ready, or entered an error
            # state due to an input (from `set_failed!`)
            delete!(state.waiting, thunk)
            if !get(state.errored, thunk, false)
                push!(state.ready, thunk)
            end
        end
    end
end

"""
Registers a future on the scheduler owning `thunk` that notifies our scheduler
once the thunk has completed execution, and registers the thunk value and error
state. Executes asynchronously to prevent cross-scheduler deadlock.
"""
function register_remote_future!(state, thunk::ThunkRef)
    future = ThunkFuture()
    remotecall_wait(thunk.id.wid, thunk, future, myid()) do thunk, future, our_id
        # Do this lazily to prevent deadlock (this other scheduler might also be holding our lock)
        errormonitor_tracked("register remote future $(thunk.id)", @async begin
            _state = EAGER_STATE[]
            h = lock(_state.lock) do
                t = unwrap_weak_checked(_state.thunk_dict[thunk.id])
                if haskey(_state.cache, t)
                    # Value is already available, set future and return
                    put!(future, _state.cache[t]; error=_state.errored[t])
                    return nothing
                end
                # Create a valid handle to access the remote scheduler
                SchedulerHandle(thunk, _state.worker_chans[our_id]...)
            end
            h === nothing && return
            register_future!(h, thunk, future)
        end)
    end

    # Get notified later
    errormonitor_tracked("listen remote future $(thunk.id)", @async begin
        # Wait for the future
        value, error = try
            (fetch(future), false)
        catch err
            (err, true)
        end
        # Save the result and schedule dependents
        lock(state.lock) do
            cache_store!(state, thunk, value, error)
            preserve_local_dependents!(state, thunk)
            schedule_dependents!(state, thunk, error)
            put!(state.chan, RescheduleSignal())
        end
    end)
end

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin, thunk=origin)
    filter!(x->x!==thunk, state.ready)
    state.cache[thunk] = ThunkFailedException(thunk, origin, state.cache[origin])
    state.errored[thunk] = true
    finish_failed!(state, thunk, origin)
end
function finish_failed!(state, thunk, origin=nothing)
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
    end
    if haskey(state.waiting, thunk)
        delete!(state.waiting, thunk)
    end
end

"Internal utility, useful for debugging scheduler state."
function print_sch_status(state; kwargs...)
    for thunk in unique(map(unwrap_weak_checked, values(state.thunk_dict)))
        print_sch_status(state, thunk; kwargs...)
    end
end
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
        elseif haskey(state.cache, node)
            status *= "C"
        else
            status *= "?"
        end
        status
    end
    if offset == 0
        println(io, "Ready ($(length(state.ready))): $(join(map(t->t.id, state.ready), ", "))")
        println(io, "Running: ($(length(state.running))): $(join(map(t->t.id, collect(state.running)), ", "))")
        print(io, "($(status_string(thunk))) ")
    end
    println(io, "$(thunk.id): $(thunk.f)")
    for (idx, input) in enumerate(thunk.syncdeps)
        if input isa WeakThunk
            input = Dagger.unwrap_weak(input)
            if input === nothing
                println(io, repeat(' ', offset+2), "[???]")
                continue
            end
        end
        if input isa ThunkRef
            println(io, repeat(' ', offset+2), "($(status_string(input))) [@$(input.id)]")
            continue
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
function signature(task::Thunk, state)
    sig = Any[chunktype(task.f)]
    for (pos, input) in collect_task_inputs(state, task)
        # N.B. Skips kwargs
        if pos === nothing
            push!(sig, chunktype(input))
        end
    end
    return sig
end

function can_use_proc(task, gproc, proc, opts, scope)
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
                @dagdebug task :scope "Rejected $proc: !(typeof(proc) in proclist)"
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
    proc_scope = Dagger.ExactScope(proc)
    if constrain(scope, proc_scope) isa Dagger.InvalidScope
        @dagdebug task :scope "Rejected $proc: Not contained in task scope ($scope)"
        return false, scope
    end

    @label accept

    @dagdebug task :scope "Accepted $proc"
    return true, scope
end

function has_capacity(state, p, gp, time_util, alloc_util, occupancy, sig)
    T = typeof(p)
    est_time_util = round(UInt64, if time_util !== nothing && haskey(time_util, T)
        time_util[T] * 1000^3
    else
        something(fetch_metric(SimpleAverageAggregator(ThreadTimeMetric()), :signature, :execute, sig), 1000^3)
    end)
    # TODO: Factor in runtime allocations as well
    est_alloc_util = if alloc_util !== nothing && haskey(alloc_util, T)
        alloc_util[T]
    else
        something(fetch_metric(SimpleAverageAggregator(ResultSizeMetric()), :signature, :execute, sig), UInt64(0))
    end::UInt64
    est_occupancy = if occupancy !== nothing && haskey(occupancy, T)
        # Clamp to 0-1, and scale between 0 and `typemax(UInt32)`
        Base.unsafe_trunc(UInt32, clamp(occupancy[T], 0, 1) * typemax(UInt32))
    else
        typemax(UInt32)
    end::UInt32
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
function collect_task_inputs(state, task)
    inputs = Pair{Union{Symbol,Nothing},Any}[]
    for (pos, input) in task.inputs
        input = unwrap_weak_checked(input)
        push!(inputs, pos => (istask(input) ? cache_lookup_checked(state, input)[1] : input))
    end
    return inputs
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
storage_safe_type(::Type{Dagger.EagerThunk}) = false
storage_safe_type(::Type{<:Chunk}) = false
storage_safe_type(::Type{MemPool.DRef}) = false
storage_safe_type(::Type{<:Ptr}) = false
storage_safe_type(::Type{<:Core.LLVMPtr}) = false
