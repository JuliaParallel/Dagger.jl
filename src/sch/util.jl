"""
    unwrap_nested_exception(err::Exception) -> Bool

Extracts the "core" exception from a nested exception."
"""
unwrap_nested_exception(err::CapturedException) =
    unwrap_nested_exception(err.ex)
unwrap_nested_exception(err::RemoteException) =
    unwrap_nested_exception(err.captured)
unwrap_nested_exception(err) = err
"Prepares the scheduler to schedule `thunk` by rescheduling all children."
function reschedule_inputs!(state, thunk)
    if !haskey(state.waiting, thunk)
        state.waiting[thunk] = Set{Thunk}()
    end
    for input in thunk.inputs
        istask(input) || continue
        push!(state.waiting_data[input], thunk)
        haskey(state.cache, input) && continue
        push!(state.waiting[thunk], input)
        reschedule_inputs!(state, input)
    end
end
