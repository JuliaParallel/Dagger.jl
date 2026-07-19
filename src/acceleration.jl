const ACCELERATION = TaskLocalValue{Acceleration}(() -> DistributedAcceleration())

current_acceleration() = ACCELERATION[]

default_processor(::DistributedAcceleration) = OSProc(myid())
default_processor(accel::DistributedAcceleration, x) = default_processor(accel)
default_processor() = default_processor(current_acceleration())

accelerate!(accel::Symbol) = accelerate!(Val{accel}())
accelerate!(::Val{:distributed}) = accelerate!(DistributedAcceleration())

function _with_default_acceleration(f)
    old_accel = ACCELERATION[]
    ACCELERATION[] = DistributedAcceleration()
    result = try
        f()
    finally
        ACCELERATION[] = old_accel
    end
    return result
end

# `do_task` runs on pooled/reusable tasks, so `ACCELERATION` (a TaskLocalValue)
# can carry over a previous task's value. Rather than wrapping the whole task
# body in a try/finally closure to restore it, each `do_task` sets it once up
# front: the value is always overwritten before anything reads it, and no other
# code runs on those pooled tasks, so a restore is unnecessary. `nothing` (an
# unstamped task) falls back to the Distributed default.
set_task_acceleration!(accel::Acceleration) = (ACCELERATION[] = accel; nothing)
set_task_acceleration!(::Nothing) = (ACCELERATION[] = DistributedAcceleration(); nothing)

initialize_acceleration!(a::DistributedAcceleration) = nothing
finalize_acceleration!(::Acceleration) = nothing

"Whether two accelerations represent the same installed backend (type + fields)."
same_acceleration(::Acceleration, ::Acceleration) = false
same_acceleration(a::T, b::T) where {T<:Acceleration} = a == b

function accelerate!(accel::Acceleration)
    old = ACCELERATION[]
    if !same_acceleration(old, accel)
        finalize_acceleration!(old)
        initialize_acceleration!(accel)
    end
    ACCELERATION[] = accel
end

accel_matches_proc(accel::DistributedAcceleration, proc::OSProc) = true
accel_matches_proc(accel::DistributedAcceleration, proc) = true

function compatible_processors(accel::Union{Acceleration,Nothing}, scope::AbstractScope, procs::Vector{<:Processor})
    comp = compatible_processors(scope, procs)
    accel === nothing && return comp
    return Set(p for p in comp if accel_matches_proc(accel, p))
end
compatible_processors(accel::Union{Acceleration,Nothing}, scope::AbstractScope=get_compute_scope(), ctx::Context=Sch.eager_context()) =
    compatible_processors(accel, scope, procs(ctx))

uniform_execution(::DistributedAcceleration) = false
uniform_execution() = uniform_execution(current_acceleration())

# SPMD uniformity checking. Under a uniform-execution acceleration (e.g. MPI),
# planning-time values are hashed and compared across ranks to catch divergence
# early. Core datadeps calls `check_uniform` unconditionally; the real checks
# live in MPIExt, so core owns the toggle plus a no-op fallback that always
# reports uniformity when no such extension is loaded.
const CHECK_UNIFORMITY = Ref{Bool}(false)
check_uniformity!(check::Bool=true) = (CHECK_UNIFORMITY[] = check)
# Generic fallback: when checking is off or execution isn't uniform (e.g. no
# MPIExt loaded), this is a no-op. Otherwise route through `hash` so MPIExt's
# `check_uniform(::Integer, ...)` performs the cross-rank comparison. MPIExt
# adds type-specific methods (more specific than this one) rather than
# overwriting it, which precompilation forbids.
check_uniform(value, original=value) =
    CHECK_UNIFORMITY[] && uniform_execution() ? check_uniform(hash(value), original) : true

# Scheduler hooks: Sch.jl calls these instead of MPI-specific symbols.
# Uniform-execution accelerations (e.g. MPI) inherit capacity/occupancy
# behavior via `uniform_execution`; move/cleanup hooks default to no-ops /
# async spawn and are overridden per backend where needed.
scheduling_ignore_capacity(accel::Acceleration) = uniform_execution(accel)

scheduling_task_occupancy(accel::Acceleration, est_occupancy::UInt32) =
    uniform_execution(accel) ? UInt32(0) : est_occupancy

# Region-end cleanup (wait_all): reclaim accel state for finished tasks in one
# batch instead of locking on every Sch finish_task!.
cleanup_task_accel!(::Acceleration, task) = nothing
cleanup_tasks_accel!(accel::Acceleration, tasks) =
    foreach(t -> cleanup_task_accel!(accel, t), tasks)

schedule_argument_move(::Acceleration, ::Integer, f::Function) = Threads.@spawn f()

"""
    bind_moved_argument(accel, original, moved) -> bound

After a scheduler argument move, choose the value stored on the Argument. The
default simply stores `moved` — the actual moved value — so a genuine `nothing`
(e.g. a `Chunk{Nothing}` from an ordinary task) reaches the kernel as `nothing`,
not as a `Chunk` the user never asked for.

Accelerations that use non-resident placeholders override this: under MPI a
`move` returning `nothing` on a non-owning rank means "data not local", so the
`Chunk` is kept to preserve SPMD-uniform type metadata (and `execute!` later
materializes it), and an owner-unwrapped `Chunk` is rewrapped to keep its
`chunktype` uniform across ranks.
"""
bind_moved_argument(::Acceleration, original, moved) = moved

"Logical execution rank of `proc` used for uniform task placement preference."
processor_owner_rank(accel::Acceleration, proc::Processor) = first(fire_order_key(proc))

"Deterministic ordering key for `proc` under uniform execution."
processor_order_key(accel::Acceleration, proc::Processor) =
    (fire_order_key(proc)..., short_name(proc))

"Preferred execution rank for `task`, or `nothing` if no preference."
task_processor_preference(accel::Acceleration, task, state) = nothing

"""
    select_processors_uniform!(procs, accel, task=nothing, state=nothing)

Re-sort `procs` with a deterministic, acceleration-dispatched key when
`uniform_execution(accel)` so every rank gets the same order (and with a
`task` the acceleration may prefer the rank owning task argument data).
When not under uniform execution, leaves `procs` unchanged.
"""
function select_processors_uniform!(procs, accel::Acceleration,
                                    task=nothing, state=nothing)
    uniform_execution(accel) || return procs
    pref = task_processor_preference(accel, task, state)
    sort!(procs, by=p -> (
        pref !== nothing && processor_owner_rank(accel, p) != pref,
        processor_order_key(accel, p)...,
    ))
    return procs
end

# Default processor grid for block assignment of new distributed arrays.
# Returns `nothing` to let the scheduler choose freely; uniform-execution
# accelerations (MPI) override this to produce a deterministic assignment
# that is identical on every rank.
default_procgrid(accel, nblocks::NTuple{N,Int}) where N = nothing

"Acceleration-specific finalization after array chunk tasks are spawned."
post_stage_array_chunks!(accel::Acceleration, tasks, eltype::Type, nd::Int) = tasks

default_processor(space::CPURAMMemorySpace) = OSProc(space.owner)
default_memory_space(accel::DistributedAcceleration) = CPURAMMemorySpace(myid())
# Chunk records must be labeled with the space the value actually resides in
# (e.g. GPU memory for device arrays)
default_memory_space(accel::DistributedAcceleration, x) = value_memory_space(x)
default_memory_space(x) = default_memory_space(current_acceleration(), x)
default_memory_space() = default_memory_space(current_acceleration())

# Whether two logical owners share a physical node (for device IPC fast paths).
# Defaults false; Distributed and MPI provide concrete methods.
same_node(::Acceleration, ::Integer, ::Integer) = false
