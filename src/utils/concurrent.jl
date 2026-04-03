"""
    ConcurrentDict{K,V}

A thread-safe dictionary wrapper around `Dict{K,V}`. Individual operations
(get, set, delete, etc.) are atomic. For compound operations that must be
atomic together, use `lock(f, d)` which passes the inner `Dict` to `f`.
"""
struct ConcurrentDict{K,V}
    dict::Dict{K,V}
    lock::ReentrantLock
end
ConcurrentDict{K,V}() where {K,V} = ConcurrentDict{K,V}(Dict{K,V}(), ReentrantLock())
function ConcurrentDict{K,V}(pairs::Pair...) where {K,V}
    d = Dict{K,V}(pairs...)
    ConcurrentDict{K,V}(d, ReentrantLock())
end
function ConcurrentDict(pairs::Pair{K,V}...) where {K,V}
    ConcurrentDict{K,V}(pairs...)
end

Base.getindex(d::ConcurrentDict, key) = @lock d.lock d.dict[key]
Base.setindex!(d::ConcurrentDict, val, key) = (@lock d.lock d.dict[key] = val; d)
Base.haskey(d::ConcurrentDict, key) = @lock d.lock haskey(d.dict, key)
function Base.delete!(d::ConcurrentDict, key)
    @lock d.lock delete!(d.dict, key)
    return d
end
Base.get(d::ConcurrentDict, key, default) = @lock d.lock get(d.dict, key, default)
Base.get!(f::Function, d::ConcurrentDict, key) = @lock d.lock get!(f, d.dict, key)
Base.get!(d::ConcurrentDict, key, default) = @lock d.lock get!(d.dict, key, default)
Base.pop!(d::ConcurrentDict, key) = @lock d.lock pop!(d.dict, key)
Base.pop!(d::ConcurrentDict, key, default) = @lock d.lock pop!(d.dict, key, default)
Base.isempty(d::ConcurrentDict) = @lock d.lock isempty(d.dict)
Base.length(d::ConcurrentDict) = @lock d.lock length(d.dict)
function Base.empty!(d::ConcurrentDict)
    @lock d.lock empty!(d.dict)
    return d
end
Base.keys(d::ConcurrentDict) = @lock d.lock collect(keys(d.dict))
Base.values(d::ConcurrentDict) = @lock d.lock collect(values(d.dict))
function Base.iterate(d::ConcurrentDict, state...)
    @lock d.lock iterate(d.dict, state...)
end

function Base.lock(f::Function, d::ConcurrentDict)
    lock(d.lock)
    try
        return f(d.dict)
    finally
        unlock(d.lock)
    end
end
Base.lock(d::ConcurrentDict) = lock(d.lock)
Base.unlock(d::ConcurrentDict) = unlock(d.lock)
Base.islocked(d::ConcurrentDict) = islocked(d.lock)

Base.sort(d::ConcurrentDict; kwargs...) = @lock d.lock sort(d.dict; kwargs...)

"""
    ConcurrentSet{T}

A thread-safe set wrapper around `Set{T}`. Individual operations are atomic.
For compound operations, use `lock(f, s)`.
"""
struct ConcurrentSet{T}
    set::Set{T}
    lock::ReentrantLock
end
ConcurrentSet{T}() where {T} = ConcurrentSet{T}(Set{T}(), ReentrantLock())

Base.push!(s::ConcurrentSet, x) = (@lock s.lock push!(s.set, x); s)
function Base.pop!(s::ConcurrentSet, x)
    @lock s.lock pop!(s.set, x)
end
Base.pop!(s::ConcurrentSet, x, default) = @lock s.lock pop!(s.set, x, default)
Base.in(x, s::ConcurrentSet) = @lock s.lock (x in s.set)
Base.isempty(s::ConcurrentSet) = @lock s.lock isempty(s.set)
Base.length(s::ConcurrentSet) = @lock s.lock length(s.set)
function Base.empty!(s::ConcurrentSet)
    @lock s.lock empty!(s.set)
    return s
end
Base.collect(s::ConcurrentSet) = @lock s.lock collect(s.set)
function Base.iterate(s::ConcurrentSet, state...)
    @lock s.lock iterate(s.set, state...)
end

function Base.lock(f::Function, s::ConcurrentSet)
    lock(s.lock)
    try
        return f(s.set)
    finally
        unlock(s.lock)
    end
end
Base.lock(s::ConcurrentSet) = lock(s.lock)
Base.unlock(s::ConcurrentSet) = unlock(s.lock)

"""
    ConcurrentVector{T}

A thread-safe vector wrapper around `Vector{T}`. Individual operations are
atomic. For compound operations, use `lock(f, v)`.
"""
struct ConcurrentVector{T}
    vec::Vector{T}
    lock::ReentrantLock
end
ConcurrentVector{T}() where {T} = ConcurrentVector{T}(Vector{T}(), ReentrantLock())
ConcurrentVector{T}(vec::Vector{T}) where {T} = ConcurrentVector{T}(vec, ReentrantLock())

Base.push!(v::ConcurrentVector, x) = (@lock v.lock push!(v.vec, x); v)
Base.popfirst!(v::ConcurrentVector) = @lock v.lock popfirst!(v.vec)

"""
    try_popfirst!(v::ConcurrentVector) -> Union{Some{T}, Nothing}

Atomically checks if the vector is non-empty and pops the first element.
Returns `Some(element)` or `nothing` if empty.
"""
function try_popfirst!(v::ConcurrentVector{T}) where T
    @lock v.lock begin
        isempty(v.vec) ? nothing : Some{T}(popfirst!(v.vec))
    end
end
Base.isempty(v::ConcurrentVector) = @lock v.lock isempty(v.vec)
Base.length(v::ConcurrentVector) = @lock v.lock length(v.vec)
Base.in(x, v::ConcurrentVector) = @lock v.lock (x in v.vec)
function Base.filter!(f, v::ConcurrentVector)
    @lock v.lock filter!(f, v.vec)
    return v
end
function Base.append!(v::ConcurrentVector, items)
    @lock v.lock append!(v.vec, items)
    return v
end
function Base.empty!(v::ConcurrentVector)
    @lock v.lock empty!(v.vec)
    return v
end
function Base.iterate(v::ConcurrentVector, state...)
    @lock v.lock iterate(v.vec, state...)
end

function Base.lock(f::Function, v::ConcurrentVector)
    lock(v.lock)
    try
        return f(v.vec)
    finally
        unlock(v.lock)
    end
end
Base.lock(v::ConcurrentVector) = lock(v.lock)
Base.unlock(v::ConcurrentVector) = unlock(v.lock)
