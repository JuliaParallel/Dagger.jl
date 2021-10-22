"""
    unwrap_nested_exception(err::Exception) -> Bool

Extracts the "core" exception from a nested exception."
"""
unwrap_nested_exception(err::CapturedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::RemoteException) =
    unwrap_nested_exception(err.captured)
unwrap_nested_exception(err) = err

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
    for inp in unwrap_weak_checked.(node.inputs)
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

fn_type(x::Chunk) = x.chunktype
fn_type(x) = typeof(x)
function signature(task::Thunk, state)
    inputs = map(x->istask(x) ? state.cache[x] : x, unwrap_weak_checked.(task.inputs))
    Tuple{fn_type(task.f), map(x->x isa Chunk ? x.chunktype : typeof(x), inputs)...}
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
