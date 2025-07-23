"""
    SchedulerOptions

Stores DAG-global options to be passed to the Dagger.Sch scheduler.

# Arguments
- `single::Int=0`: (Deprecated) Force all work onto worker with specified id.
  `0` disables this option.
- `proclist=nothing`: (Deprecated) Force scheduler to use one or more
  processors that are instances/subtypes of a contained type. Alternatively, a
  function can be supplied, and the function will be called with a processor as
  the sole argument and should return a `Bool` result to indicate whether or not
  to use the given processor. `nothing` enables all default processors.
- `allow_errors::Bool=false`: Allow thunks to error without affecting
  non-dependent thunks.
- `checkpoint=nothing`: If not `nothing`, uses the provided function to save
  the final result of the current scheduler invocation to persistent storage, for
  later retrieval by `restore`.
- `restore=nothing`: If not `nothing`, uses the provided function to return the
  (cached) final result of the current scheduler invocation, were it to execute.
  If this returns a `Chunk`, all thunks will be skipped, and the `Chunk` will be
  returned.  If `nothing` is returned, restoring is skipped, and the scheduler
  will execute as usual. If this function throws an error, restoring will be
  skipped, and the error will be displayed.
"""
Base.@kwdef struct SchedulerOptions
    single::Union{Int,Nothing} = nothing
    proclist = nothing
    allow_errors::Union{Bool,Nothing} = false
    checkpoint = nothing
    restore = nothing
end
