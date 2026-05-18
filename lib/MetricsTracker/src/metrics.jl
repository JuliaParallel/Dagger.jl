export @with_metrics, with_metrics

function with_metrics(f, ms::MetricsSpec, mod::Module, context::Symbol, key, sync_loc::SyncLocation)
    @assert !COLLECTING_METRICS[] "Nested metrics collection not yet supported"

    ctx_val = Val{context}()
    start_values = ntuple(length(ms.metrics)) do i
        m = ms.metrics[i]
        if metric_applies(m, ctx_val) && !is_result_metric(m)
            return start_metric(m)
        else
            return nothing
        end
    end

    result = nothing
    try
        result = @with COLLECTING_METRICS => true METRIC_REGION => (mod, context) METRIC_KEY => Some{Any}(key) f()
        return result
    finally
        n = length(ms.metrics)
        final_values = ntuple(n) do i
            j = n - i + 1
            m = ms.metrics[j]
            if metric_applies(m, ctx_val)
                if is_result_metric(m) && result !== nothing
                    return result_metric(m, result)
                else
                    return stop_metric(m, start_values[j])
                end
            else
                return nothing
            end
        end
        final_values = reverse(final_values)
        commit_metric_values!(ms, mod, context, key, sync_loc, final_values)
    end
end

function with_metrics(f, ms::Tuple, mod::Module, context::Symbol, key, sync_loc::SyncLocation)
    return with_metrics(f, MetricsSpec(ms...), mod, context, key, sync_loc)
end

macro with_metrics(ms, context, key, sync_loc, body)
    return esc(quote
        $with_metrics(() -> $body, $ms, $__module__, $context, $key, $sync_loc)
    end)
end

macro with_metrics(ms, mod, context, key, sync_loc, body)
    return esc(quote
        $with_metrics(() -> $body, $ms, $mod, $context, $key, $sync_loc)
    end)
end

function commit_metric_values!(ms::MetricsSpec, mod::Module, context::Symbol,
                               key, ::SyncTask, values::Tuple)
    cache = local_metrics_cache()
    apply_values!(cache, ms, mod, context, key, values)
    return
end

function commit_metric_values!(ms::MetricsSpec, mod::Module, context::Symbol,
                               key, ::SyncGlobal, values::Tuple)
    apply_values!(GLOBAL_METRICS_CACHE, ms, mod, context, key, values)
    return
end

function commit_metric_values!(ms::MetricsSpec, mod::Module, context::Symbol,
                               key, sync_loc::SyncInto, values::Tuple)
    apply_values!(sync_loc.cache, ms, mod, context, key, values)
    return
end

function apply_values!(cache::MetricsCache, ms::MetricsSpec, mod::Module, context::Symbol,
                       key, values::Tuple)
    journal = get_journal(cache)
    bulk_update!(cache) do c
        ctx = pending_context!(c, mod, context)
        for i in 1:length(ms.metrics)
            v = values[i]
            v === nothing && continue
            m = ms.metrics[i]
            storage = get_or_create_storage!(ctx, m)
            storage.data[key] = v
            if journal !== nothing
                append_journal!(journal, (mod, context), m, key, v)
            end
        end
    end
    return
end
