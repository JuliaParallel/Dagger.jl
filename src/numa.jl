# Where do Julia's threads run, and pinning them to the CPUs the process is allowed to use (Linux only). Julia does not pin
# its threads itself; `JULIA_EXCLUSIVE=1` pins thread `i` to CPU `i-1` and ignores the process's CPU mask.

"""
    thread_cpus() -> Vector{Int}

The CPU (Linux numbering, `sched_getcpu`) each Julia thread of the default pool is running on right now,
indexed by `Threads.threadid()`; `-1` for a thread that could not be asked. Empty off Linux. Sampled with a
task pinned to each thread, so it costs one scheduling round trip per thread; call it once at startup.
Threads that are not pinned may move between CPUs, so the answer is only a snapshot.
"""
function thread_cpus()
    Sys.islinux() || return Int[]
    cpus = fill(-1, Threads.maxthreadid())
    tasks = Task[]
    for tid in 1:Threads.maxthreadid()
        Threads.threadpool(tid) === :default || continue
        t = Task(() -> (cpus[Threads.threadid()] = Int(ccall(:sched_getcpu, Cint, ()))))
        set_task_tid!(t, tid)
        schedule(t)
        push!(tasks, t)
    end
    foreach(wait, tasks)
    return cpus
end

"""
    allowed_cpus() -> Vector{Int}

The CPUs this process may run on (Linux `sched_getaffinity`: what `numactl`, `taskset`, Open MPI `--bind-to` or the
batch system allow), ascending. Empty off Linux.
"""
function allowed_cpus()
    Sys.islinux() || return Int[]
    return _allowed_cpus((nbytes, buf) -> ccall(:sched_getaffinity, Cint, (Cint, Csize_t, Ptr{UInt8}), 0, nbytes, buf))
end

# The CPU numbers set in an affinity mask: bit `b` of byte `i` (both from 0) is CPU `8i + b`.
_decode_cpu_mask(buf::AbstractVector{UInt8}) = Int[8 * (i - 1) + b for i in eachindex(buf) for b in 0:7 if (buf[i] >> b) & 1 == 1]

# `getaffinity(nbytes, buf)` fills `buf` and returns 0, or fails (nonzero) when the mask does not fit.
# The kernel rejects a mask smaller than its own CPU count (EINVAL), so grow the buffer until it fits: it starts with room
# for 1024 CPUs and goes up to 524288.
function _allowed_cpus(getaffinity)
    nbytes = 128
    while nbytes <= 1 << 16
        buf = zeros(UInt8, nbytes)
        getaffinity(nbytes, buf) == 0 && return _decode_cpu_mask(buf)
        nbytes *= 2
    end
    return Int[]
end

# An affinity mask with only `cpu` set: a whole number of 8-byte words (what the kernel takes), at least room for 1024 CPUs.
function _cpu_mask(cpu::Integer)
    cpu >= 0 || throw(ArgumentError("invalid CPU number $cpu"))
    mask = zeros(UInt8, max(128, 8 * cld((cpu >> 3) + 1, 8)))
    mask[(cpu >> 3) + 1] = UInt8(1) << (cpu & 7)
    return mask
end

# The CPU each of the threads `tids` is to be pinned to: the first ones of `cpus`.
function _pin_targets(tids::AbstractVector{<:Integer}, cpus::AbstractVector{<:Integer})
    length(cpus) >= length(tids) || throw(ArgumentError("$(length(tids)) threads but only $(length(cpus)) CPUs to pin them to"))
    for cpu in @view cpus[1:length(tids)]
        cpu >= 0 || throw(ArgumentError("invalid CPU number $cpu"))
    end
    return Int[cpus[k] for k in eachindex(tids)]
end

"""
    pin_threads!(; cpus=allowed_cpus()) -> Vector{Int}

Pin the default-pool Julia threads, in thread-id order, one to each of `cpus` (Linux), and return the resulting
[`thread_cpus`](@ref). Use it where `JULIA_EXCLUSIVE=1` is wrong: that pins thread `i` to CPU `i-1` and ignores the
process's CPU mask, so several ranks on a node would all pin onto the same cores. Here the default is the CPUs the
launcher allowed, so a rank bound to its own cores or NUMA domain stays inside it. Throws when there are fewer CPUs
than threads. Call it once at startup, before the threads do work; it does nothing off Linux.
"""
function pin_threads!(; cpus::Vector{Int}=allowed_cpus())
    Sys.islinux() || return Int[]
    tids = [tid for tid in 1:Threads.maxthreadid() if Threads.threadpool(tid) === :default]
    targets = _pin_targets(tids, cpus)
    tasks = Task[]
    for (tid, cpu) in zip(tids, targets)
        t = Task() do
            mask = _cpu_mask(cpu)
            # pid 0: the calling thread
            ccall(:sched_setaffinity, Cint, (Cint, Csize_t, Ptr{UInt8}), 0, length(mask), mask) == 0
        end
        set_task_tid!(t, tid)
        schedule(t)
        push!(tasks, t)
    end
    errs = [tid for (tid, t) in zip(tids, tasks) if !fetch(t)::Bool]
    isempty(errs) || @warn "Could not pin threads" errs
    return thread_cpus()
end
