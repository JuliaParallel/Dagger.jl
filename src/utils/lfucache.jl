struct BasicLFUCache{K,V}
    cache::Dict{K,V}
    freq::Dict{K,Int}
    max_size::Int

    BasicLFUCache{K,V}(max_size::Int) where {K,V} = new(Dict{K,V}(), Dict{K,Int}(), max_size)
end
function Base.empty!(cache::BasicLFUCache)
    empty!(cache.cache)
    empty!(cache.freq)
    return cache
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
        # `findmin(::Dict)` boxes its (frequency, key) result. Scan directly
        # and keep the first minimum, preserving its iteration-order ties.
        lfu_key, min_freq = first(cache.freq)
        for (candidate, frequency) in cache.freq
            if frequency < min_freq
                lfu_key = candidate
                min_freq = frequency
            end
        end
        delete!(cache.cache, lfu_key)
        delete!(cache.freq, lfu_key)
    end
    return val
end
