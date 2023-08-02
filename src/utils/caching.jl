"""
    mem_limit(total::UInt, percentage_limit::Int, nprocs::Int)

Returns the per process mem limit in MiB based on the provided `total` memory,
`percentage_limit` (1-100) and the number of processes in the current setup.
"""
function mem_limit(total::UInt, percentage_limit::Int, nprocs::Int)
    return total * percentage_limit / 100 ÷ nprocs
end

"""
    enable_disk_caching!(ram_percentage_limit::Int=30, disk_limit_mb::Int=16*2^10, processes::Vector{Int}=procs())

Sets up disk caching on all processes available in the environment according to the provided
limits. The user should provide the percentage, which will decide what's the memory limit on
each participating machine (differentiated by hostname). The disk limit is set strictly per
process and doesn't include any hostname related logic.
"""
function enable_disk_caching!(
    ram_percentage_limit::Int=30,
    disk_limit_mb::Int=16*2^10,
    processes::Vector{Int}=procs(),
)
    if !(0 < ram_percentage_limit <= 100)
        throw(ArgumentError("Ram limit values must be in (1, 100] range"))
    end

    total_mem = @static VERSION >= v"1.8-" ? Sys.total_physical_memory : Sys.total_memory
    process_info = [
        id => remotecall(id, total_mem) do total_mem
            return (; total_memory=total_mem(), hostname=gethostname())
        end for id in processes
    ]

    machines = Dict{NamedTuple,Vector{Int}}()
    for (id, info) in process_info
        key = fetch(info)
        if key isa NamedTuple
            machines[key] = push!(get(machines, key, Int[]), id)
        else
            @error("Error querying hardware information on process id = $id", ex = key)
        end
    end

    mem_limits = Dict{Int,Int}()
    for (info, ids) in machines
        for id in ids
            mem_limits[id] = mem_limit(info.total_memory, ram_percentage_limit, length(ids))
        end
    end

    return enable_disk_caching!(mem_limits, disk_limit_mb, processes)
end


"""
    enable_disk_caching!(mem_limits::Dict{Int,Int}, disk_limit_mb::Int=16*2^10, processes::Vector{Int}=procs())

Sets up disk caching on participating processes.
This is a low level method for applying the `mem_limits` directly onto the processes.
This skips the process discovery stage and the limit calculation.
"""
function enable_disk_caching!(
    mem_limits::Dict{Int,Int},
    disk_limit_mb::Int=16*2^10,
    processes::Vector{Int}=procs(),
)
    results = [
        remotecall(id) do
            !isdefined(Main, :Dagger) && Main.eval(:(using Dagger))
            Dagger.MemPool.setup_global_device!(
                Dagger.MemPool.DiskCacheConfig(;
                    toggle=true, membound=mem_limits[id], diskbound=disk_limit_mb * 2^20
                ),
            )
            nothing
        end for id in processes
    ]
    any_error = false
    for (i, id) in enumerate(processes)
        r = fetch(results[i])
        any_error |= r !== nothing
        if r !== nothing
            @error("Error setting up disk caching on process id = $id", ex = r)
        end
    end

    return if any_error
        @error("Disk cache setup failed")
        false
    else
        true
    end
end
