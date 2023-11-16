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
Base.lock(x::LockedObject) = lock(x.lock)
Base.trylock(x::LockedObject) = trylock(x.lock)
Base.unlock(x::LockedObject) = unlock(x.lock)
payload(x::LockedObject) = x.payload

# TODO: Move these back to MemPool
macro safe_lock1(l, o, ex)
    quote
        temp = $(esc(l))
        lock(temp)
        MemPool.enable_finalizers(false)
        try
            $(esc(o)) = $payload(temp)
            $(esc(ex))
        finally
            unlock(temp)
            MemPool.enable_finalizers(true)
        end
    end
end
# If we actually want to acquire a lock from a finalizer, we can't cause a task
# switch. As a NonReentrantLock can only be taken by another thread that should
# be running, and not a concurrent task we'd need to switch to, we can safely
# spin.
macro safe_lock_spin1(l, o, ex)
    quote
        temp = $(esc(l))
        while !trylock(temp)
            # we can't yield here
            GC.safepoint()
        end
        MemPool.enable_finalizers(false) # retains compatibility with non-finalizer callers
        try
            $(esc(o)) = $payload(temp)
            $(esc(ex))
        finally
            unlock(temp)
            MemPool.enable_finalizers(true)
        end
    end
end
