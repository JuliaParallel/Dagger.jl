# Copied from CUDA.jl

struct LockedObject{T}
    lock::ReentrantLock
    payload::T
end
LockedObject(payload) = LockedObject(Threads.ReentrantLock(), payload)

function Base.lock(f, x::LockedObject)
    lock(x.lock)
    try
        return f(x.payload)
    finally
        unlock(x.lock)
    end
end
