using Dagger
include("ext/JSON3Ext.jl") # This brings JSON3 into scope for the Dagger extension

# Create dummy logs Dict for testing
# The structure aims to be compatible with what Dagger.logs_event_pairs and
# logs_to_chrome_trace expect.
# We use NamedTuples to simulate the structure of internal Dagger log event types.

# Assume Dagger.ThreadProc is an accessible type. This is a potential failure point.
# If this script fails due to Dagger.ThreadProc, we'll need to investigate how to correctly reference or mock it.
dummy_processor = Dagger.ThreadProc(1, 1) # (owner, tid)

dummy_logs = Dict{Any,Any}(
    # Entries for one worker, "w1"
    "w1" => Dict{Symbol,Any}(
        :core => [
            (kind=:start, category=:compute, timestamp=0.0, processor=dummy_processor, thunk_id=101), # start event for thunk 101
            (kind=:finish, category=:compute, timestamp=10.0, processor=dummy_processor, thunk_id=101), # finish event for thunk 101
            # Add 'kind' to :add_thunk event as well.
            (kind=:event, category=:add_thunk, timestamp=0.0, processor=dummy_processor, thunk_id=101),
        ],
        :id => [
            (thunk_id=101, processor=dummy_processor), # Corresponds to first :core entry
            (thunk_id=101, processor=dummy_processor), # Corresponds to second :core entry
            (thunk_id=101, processor=dummy_processor), # Corresponds to third :core entry

        ],
        :taskfuncnames => [
            "actual_task_name_for_101", # for :add_thunk category
            "ignored_for_compute",
            "ignored_for_compute",
        ],
        # For :add_thunk, if :taskuidtotid is present, it's used.
        # Let's simulate a case where it's not used or maps to the same thunk_id
        :taskuidtotid => [
            nothing, # or (UInt(1) => 101) if we wanted to test that path
            nothing,
            nothing,
        ]
    )
)

# Call the deprecated function
println("Calling Dagger.render_logs, expecting a deprecation warning...")
result = Dagger.render_logs(dummy_logs, Val(:chrome_trace))
println("Dagger.render_logs called.")

# Basic check on the result (should be a JSON string)
if result isa String && length(result) > 0 && result[1] == '{'
    println("Test partially successful: render_logs returned a JSON string.")
else
    println("Test failed: render_logs did not return a valid JSON string. Result was: ", result)
end
