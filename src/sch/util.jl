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
function get_propagated_options(thunk)
    nt = NamedTuple()
    for key in thunk.propagates
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
    nt
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

"Cleans up any syncdeps that aren't needed any longer, and returns a
`Set{Chunk}` of all chunks that can now be evicted from workers."
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
                if istask(inp) && haskey(state.cache, inp)
                    _node = state.cache[inp]
                    if _node isa Chunk
                        push!(to_evict, _node)
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
    for dep in sort!(collect(get(()->Set{Thunk}(), state.waiting_data, node)), by=state.node_order)
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

"""
Prepares the scheduler to schedule `thunk`. Will mark `thunk` as ready if
its inputs are satisfied.
"""
function reschedule_syncdeps!(state, thunk, seen=Set{Thunk}())
    to_visit = Thunk[thunk]
    while !isempty(to_visit)
        thunk = pop!(to_visit)
        push!(seen, thunk)
        if haskey(state.valid, thunk)
            continue
        end
        if haskey(state.cache, thunk) || (thunk in state.ready) || (thunk in state.running)
            continue
        end
        for (_,input) in thunk.inputs
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
        for input in thunk.syncdeps
            input = unwrap_weak_checked(input)
            istask(input) && input in seen && continue

            # Unseen
            push!(get!(()->Set{Thunk}(), state.waiting_data, input), thunk)
            istask(input) || continue

            # Unseen task
            if get(state.errored, input, false)
                set_failed!(state, input, thunk)
            end
            haskey(state.cache, input) && continue

            # Unseen and unfinished task
            push!(w, input)
            if !((input in state.running) || (input in state.ready))
                push!(to_visit, input)
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

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin, thunk=origin)
    filter!(x->x!==thunk, state.ready)
    ex = state.cache[origin]
    if ex isa RemoteException
        ex = ex.captured
    end
    state.cache[thunk] = DTaskFailedException(thunk, origin, ex)
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
        println(io, "Ready ($(length(state.ready))): $(join(map(t->t.id, state.ready), ','))")
        println(io, "Running: ($(length(state.running))): $(join(map(t->t.id, collect(state.running)), ','))")
        print(io, "($(status_string(thunk))) ")
    end
    println(io, "$(thunk.id): $(thunk.f)")
    for (idx, input) in enumerate(thunk.syncdeps)
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
    signature(task.f, collect_task_inputs(state, task.inputs))
function signature(f, args)
    sig = DataType[chunktype(f)]
    sig_kwarg_names = Symbol[]
    sig_kwarg_types = []
    for (pos, arg) in args
        if arg isa Dagger.DTask
            # Only occurs via manual usage of signature
            arg = fetch(arg; raw=true)
        end
        T = chunktype(arg)
        if pos === nothing
            push!(sig, T)
        else
            push!(sig_kwarg_names, pos)
            push!(sig_kwarg_types, T)
        end
    end
    if !isempty(sig_kwarg_names)
        NT = NamedTuple{(sig_kwarg_names...,), Base.to_tuple_type(sig_kwarg_types)}
        pushfirst!(sig, NT)
        @static if isdefined(Core, :kwcall)
            pushfirst!(sig, typeof(Core.kwcall))
        else
            f_instance = chunktype(f).instance
            kw_f = Core.kwfunc(f_instance)
            pushfirst!(sig, typeof(kw_f))
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
    # FIXME: MaxUtilization
    est_time_util = round(UInt64, if time_util !== nothing && haskey(time_util, T)
        time_util[T] * 1000^3
    else
        get(state.signature_time_cost, sig, 1000^3)
    end)
    est_alloc_util = if alloc_util !== nothing && haskey(alloc_util, T)
        alloc_util[T]
    else
        get(state.signature_alloc_cost, sig, UInt64(0))
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

function populate_processor_cache_list!(state, procs)
    # Populate the cache if empty
    if state.procs_cache_list[] === nothing
        current = nothing
        for p in map(x->x.pid, procs)
            for proc in get_processors(OSProc(p))
                next = ProcessorCacheEntry(OSProc(p), proc)
                if current === nothing
                    current = next
                    current.next = current
                    state.procs_cache_list[] = current
                else
                    current.next = next
                    current = next
                    current.next = state.procs_cache_list[]
                end
            end
        end
    end
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
collect_task_inputs(state, task::Thunk) =
    collect_task_inputs(state, task.inputs)
function collect_task_inputs(state, inputs)
    new_inputs = Pair{Union{Symbol,Nothing},Any}[]
    for (pos, input) in inputs
        input = unwrap_weak_checked(input)
        push!(new_inputs, pos => (istask(input) ? state.cache[input] : input))
    end
    return new_inputs
end

"""
Estimates the cost of scheduling `task` on each processor in `procs`. Considers
current estimated per-processor compute pressure, and transfer costs for each
`Chunk` argument to `task`. Returns `(procs, costs)`, with `procs` sorted in
order of ascending cost.
"""
function estimate_task_costs(state, procs, task, inputs)
    tx_rate = state.transfer_rate[]

    # Find all Chunks
    chunks = Chunk[]
    for input in inputs
        if input isa Chunk
            push!(chunks, input)
        end
    end

    costs = Dict{Processor,Float64}()
    for proc in procs
        chunks_filt = Iterators.filter(c->get_parent(processor(c))!=get_parent(proc), chunks)

        # Estimate network transfer costs based on data size
        # N.B. `affinity(x)` really means "data size of `x`"
        # N.B. We treat same-worker transfers as having zero transfer cost
        # TODO: For non-Chunk, model cost from scheduler to worker
        # TODO: Measure and model processor move overhead
        tx_cost = impute_sum(affinity(chunk)[2] for chunk in chunks_filt)

        # Estimate total cost to move data and get task running after currently-scheduled tasks
        est_time_util = get(state.worker_time_pressure[get_parent(proc).pid], proc, 0)
        costs[proc] = est_time_util + (tx_cost/tx_rate)
    end

    # Shuffle procs around, so equally-costly procs are equally considered
    P = randperm(length(procs))
    procs = getindex.(Ref(procs), P)

    # Sort by lowest cost first
    sort!(procs, by=p->costs[p])

    return procs, costs
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
