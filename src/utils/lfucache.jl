struct BasicLFUCache{K,V}
    cache::Dict{K,V}
    freq::Dict{K,Int}
    max_size::Int

    BasicLFUCache{K,V}(max_size::Int) where {K,V} = new(Dict{K,V}(), Dict{K,Int}(), max_size)
end
function Base.get!(f, cache::BasicLFUCache{K,V}, key::K) where {K,V}
    if haskey(cache.cache, key)
        cache.freq[key] += 1
        return cache.cache[key]
    end
    val = f()::V
    cache.cache[key] = val
    cache.freq[key] = 1
    if length(cache.cache) > cache.max_size
        # Find the least frequently used key
        _, lfu_key::K = findmin(cache.freq)
        delete!(cache.cache, lfu_key)
        delete!(cache.freq, lfu_key)
    end
    return val
end