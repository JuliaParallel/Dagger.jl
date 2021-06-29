"""
    unwrap_nested_exception(err::Exception) -> Bool

Extracts the "core" exception from a nested exception."
"""
unwrap_nested_exception(err::CapturedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::RemoteException) =
    unwrap_nested_exception(err.captured)
unwrap_nested_exception(err) = err

"Prepares the scheduler to schedule `thunk`."
function reschedule_inputs!(state, thunk, seen=Dict{Thunk,Bool}())
    haskey(seen, thunk) && return seen[thunk]
    haskey(state.cache, thunk) && return false
    w = get!(()->Set{Thunk}(), state.waiting, thunk)
    scheduled = false
    for input in unwrap_weak.(thunk.inputs)
        if istask(input) || (input isa Chunk)
            push!(get!(()->Set{Thunk}(), state.waiting_data, input), thunk)
        end
        istask(input) || continue
        if get(state.errored, input, false)
            set_failed!(state, input, thunk)
            scheduled = true
            break # TODO: Allow collecting all error'd inputs
        end
        haskey(state.cache, input) && continue
        if (input in state.running) ||
           (input in state.ready) ||
           reschedule_inputs!(state, input, seen)
            push!(w, input)
            scheduled = true
        else
            error("Failed to reschedule $(input.id) for $(thunk.id)")
        end
    end
    if isempty(w)
        # Inputs are ready
        delete!(state.waiting, thunk)
        if !get(state.errored, thunk, false)
            get!(()->Set{Thunk}(), state.waiting_data, thunk)
            push!(state.ready, thunk)
        end
        return true
    else
        return scheduled
    end
end

"Marks `thunk` and all dependent thunks as failed."
function set_failed!(state, origin, thunk=origin)
    filter!(x->x!==thunk, state.ready)
    state.cache[thunk] = ThunkFailedException(thunk, origin, state.cache[origin])
    state.errored[thunk] = true
    if haskey(state.futures, thunk)
        for future in state.futures[thunk]
            put!(future, state.cache[thunk]; error=true)
        end
        delete!(state.futures, thunk)
    end
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
            stk = Base.catch_stack(task)
            err, frames = stk[1]
            rethrow(CapturedException(err, frames))
        else
            rethrow(task.result)
        end
    end
end

function signature(task::Thunk, state)
    inputs = map(x->istask(x) ? state.cache[x] : x, unwrap_weak.(task.inputs))
    Tuple{typeof(task.f), map(x->x isa Chunk ? x.chunktype : typeof(x), inputs)...}
end
