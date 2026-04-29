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

initialize_acceleration!(a::DistributedAcceleration) = nothing
function accelerate!(accel::Acceleration)
    initialize_acceleration!(accel)
    ACCELERATION[] = accel
end
accelerate!(::Nothing) = nothing

accel_matches_proc(accel::DistributedAcceleration, proc::OSProc) = true
accel_matches_proc(accel::DistributedAcceleration, proc) = true

function compatible_processors(accel::Union{Acceleration,Nothing}, scope::AbstractScope, procs::Vector{<:Processor})
    comp = compatible_processors(scope, procs)
    accel === nothing && return comp
    return Set(p for p in comp if accel_matches_proc(accel, p))
end

uniform_execution(::DistributedAcceleration) = false
uniform_execution() = uniform_execution(current_acceleration())

default_processor(space::CPURAMMemorySpace) = OSProc(space.owner)
default_memory_space(accel::DistributedAcceleration) = CPURAMMemorySpace(myid())
default_memory_space(accel::DistributedAcceleration, x) = default_memory_space(accel)
default_memory_space(x) = default_memory_space(current_acceleration(), x)
default_memory_space() = default_memory_space(current_acceleration())
