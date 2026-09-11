# Statically declared log categories for Dagger's hot paths. Call sites use
# `@logstart` / `@logfinish` so ids are concrete structs rather than
# NamedTuples. `as_old_id` is generated from the id fields; `old_data`
# wraps a bare start/finish payload into the NamedTuple shape that
# existing Events.* consumers expect.

TimespanLogging.@logcategory LogCompute as=:compute id=(thunk_id::Int, processor::Any) old_data=(:f, :result)
TimespanLogging.@logcategory LogMove as=:move id=(thunk_id::Int, position::Any, processor::Any, id::Any) old_data=:data
TimespanLogging.@logcategory LogTake as=:take id=(uid::UInt,) data=Nothing
TimespanLogging.@logcategory LogProcRunWait as=:proc_run_wait id=(uid::UInt, worker::Int, processor::Any) data=Nothing
TimespanLogging.@logcategory LogProcRunFetch as=:proc_run_fetch id=(uid::UInt, worker::Int, processor::Any) old_data=:thunk_id
TimespanLogging.@logcategory LogDatadepsCopy as=:datadeps_copy id=(id::UInt,)
TimespanLogging.@logcategory LogDatadepsCopySkip as=:datadeps_copy_skip id=(id::UInt,)
TimespanLogging.@logcategory LogDatadepsExecute as=:datadeps_execute id=(thunk_id::UInt,)
TimespanLogging.@logcategory LogAddThunk as=:add_thunk id=(thunk_id::Int,)
TimespanLogging.@logcategory LogFinish as=:finish id=(uid::UInt, thunk_id::Int)
TimespanLogging.@logcategory LogEnqueue as=:enqueue id=(uid::UInt, processor::Any, thunk_id::Int) data=Nothing
TimespanLogging.@logcategory LogSchedule as=:schedule id=(uid::UInt, thunk_id::Int)
TimespanLogging.@logcategory LogFire as=:fire id=(uid::UInt, worker::Int) data=Nothing

# Hierarchical datadeps planning instrumentation -- see `hier_log!` in
# datadeps/hierarchical.jl. Gated by `Dagger.HIER_TIMING[]`, not the shared
# `enable!` bits, so these three are declared (and their ids constructed) even
# when the rest of logging is off.
TimespanLogging.@logcategory LogHierPhase as=:hier_phase id=(phase::Symbol,) data=Nothing
TimespanLogging.@logcategory LogHierSlot as=:hier_slot id=(kind::Symbol,) data=UInt64
TimespanLogging.@logcategory LogHierAinfo as=:hier_ainfo id=() data=Tuple{UInt64,Int}
