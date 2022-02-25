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
            isa(thunk.f, Chunk) ? thunk.f.scope : AnyScope()
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

"Cleans up any inputs that aren't needed any longer, and returns a `Set{Chunk}`
of all chunks that can now be evicted from workers."
function cleanup_inputs!(state, node)
    to_evict = Set{Chunk}()
    for inp in map(unwrap_weak_checked, node.inputs)
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
    to_evict
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
Prepares the scheduler to schedule `thunk`, including scheduling `thunk` if
its inputs are satisfied.
"""
function reschedule_inputs!(state, thunk, seen=Set{Thunk}())
    thunk in seen && return
    push!(seen, thunk)
    if haskey(state.cache, thunk) || (thunk in state.ready) || (thunk in state.running)
        return
    end
    w = get!(()->Set{Thunk}(), state.waiting, thunk)
    for input in thunk.inputs
        input = unwrap_weak_checked(input)
        if istask(input) || (input isa Chunk)
            push!(get!(()->Set{Thunk}(), state.waiting_data, input), thunk)
        end
        istask(input) || continue
        if get(state.errored, input, false)
            set_failed!(state, input, thunk)
        end
        haskey(state.cache, input) && continue
        push!(w, input)
        if !((input in state.running) || (input in state.ready))
            reschedule_inputs!(state, input, seen)
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

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin, thunk=origin)
    filter!(x->x!==thunk, state.ready)
    state.cache[thunk] = ThunkFailedException(thunk, origin, state.cache[origin])
    state.errored[thunk] = true
    fill_registered_futures!(state, thunk, true)
    if haskey(state.waiting_data, thunk)
        for dep in state.waiting_data[thunk]
            haskey(state.waiting, dep) &&
                delete!(state.waiting, dep)
            haskey(state.errored, dep) &&
                continue
            set_failed!(state, origin, dep)
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
    for (idx,input) in enumerate(thunk.inputs)
        if input isa WeakThunk
            input = unwrap_weak(input)
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
        @static if VERSION >= v"1.1"
            @static if VERSION < v"1.7-rc1"
                stk = Base.catch_stack(task)
            else
                stk = Base.current_exceptions(task)
            end
            err, frames = stk[1]
            rethrow(CapturedException(err, frames))
        else
            rethrow(task.result)
        end
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

fn_type(x::Chunk) = x.chunktype
fn_type(x) = typeof(x)
function signature(task::Thunk, state)
    inputs = map(x->istask(x) ? state.cache[x] : x, map(unwrap_weak_checked, task.inputs))
    Any[fn_type(task.f), map(x->x isa Chunk ? x.chunktype : typeof(x), inputs)...]
end

function can_use_proc(task, gproc, proc, opts, scope)
    # Check against proclist
    if opts.proclist === nothing
        if !default_enabled(proc)
            return false
        end
    elseif opts.proclist isa Function
        if !Base.invokelatest(opts.proclist, proc)
            return false
        end
    elseif opts.proclist isa Vector
        if !(typeof(proc) in opts.proclist)
            return false
        end
    else
        throw(SchedulingException("proclist must be a Function, Vector, or nothing"))
    end

    # Check against single
    if opts.single != 0
        if gproc.pid != opts.single
            return false
        end
    end

    # Check scope
    if constrain(scope, Dagger.ExactScope(proc)) isa Dagger.InvalidScope
        return false
    end

    return true
end

function has_capacity(state, p, gp, procutil, sig)
    T = typeof(p)
    # FIXME: MaxUtilization
    extra_util = round(UInt64, get(procutil, T, 1) * 1e9)
    real_util = state.worker_pressure[gp][p]
    if (T === Dagger.ThreadProc) && haskey(state.function_cost_cache, sig)
        # Assume that the extra pressure is between estimated and measured
        # TODO: Generalize this to arbitrary processor types
        extra_util = min(extra_util, state.function_cost_cache[sig])
    end
    # TODO: update real_util based on loadavg
    cap = typemax(UInt64)
    #= TODO
    cap = state.worker_capacity[gp][T]
    if ((extra_util isa MaxUtilization) && (real_util > 0)) ||
       ((extra_util isa Real) && (extra_util + real_util > cap))
        return false, cap, extra_util
    end
    =#
    return true, cap, extra_util
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
    length(xs) == 0 && return 0

    total = zero(eltype(xs))
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

    total + nothing_count * total / something_count
end

"""
Estimates the cost of scheduling `task` on each processor in `procs`. Considers
current estimated per-processor compute pressure, and transfer costs for each
`Chunk` argument to `task`. Returns `(procs, costs)`, with `procs` sorted in
order of ascending cost.
"""
function estimate_task_costs(state, procs, task)
    tx_rate = state.transfer_rate[]

    # Find all Chunks
    inputs = map(input->istask(input) ? state.cache[input] : input, map(unwrap_weak_checked, task.inputs))
    chunks = convert(Vector{Chunk}, filter(t->isa(t, Chunk), [inputs...]))

    # Estimate network transfer costs based on data size
    # N.B. `affinity(x)` really means "data size of `x`"
    # N.B. We treat same-worker transfers as having zero transfer cost
    # TODO: For non-Chunk, model cost from scheduler to worker
    # TODO: Measure and model processor move overhead
    transfer_costs = Dict(proc=>impute_sum([affinity(chunk)[2] for chunk in filter(c->get_parent(processor(c))!=get_parent(proc), chunks)]) for proc in procs)

    # Estimate total cost to move data and get task running after currently-scheduled tasks
    costs = Dict(proc=>state.worker_pressure[get_parent(proc).pid][proc]+(tx_cost/tx_rate) for (proc, tx_cost) in transfer_costs)

    # Shuffle procs around, so equally-costly procs are equally considered
    P = randperm(length(procs))
    procs = getindex.(Ref(procs), P)

    # Sort by lowest cost first
    sort!(procs, by=p->costs[p])

    return procs, costs
end
