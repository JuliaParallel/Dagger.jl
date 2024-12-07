import Serialization: serialize, deserialize

function load_metrics!(path::String)
    loaded_cache = deserialize(path)
    global_metrics_cache() do cache
        for (mod_context, all_metrics) in loaded_cache
            inner_cache = get!(cache, mod_context) do
                Dict{Any, Dict{AbstractMetric, Any}}()
            end
            for (key, keyed_metrics) in all_metrics
                inner_keyed_cache = get!(inner_cache, key) do
                    Dict{AbstractMetric, Any}()
                end
                for (metric, value) in keyed_metrics
                    inner_keyed_cache[metric] = value
                end
            end
        end
        return cache
    end
end
function save_metrics(path::String)
    global_metrics_cache() do cache
        serialize(path, cache)
        return cache
    end
end
