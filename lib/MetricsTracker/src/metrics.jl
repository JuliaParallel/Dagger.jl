export @with_metrics, with_metrics

function with_metrics(f, ms::MetricsSpec, mod::Module, context::Symbol, key, sync_loc::SyncLocation)
    @assert !COLLECTING_METRICS[] "Nested metrics collection not yet supported"

    ctx_val = Val{context}()
    # `map` over the (heterogeneous) metrics tuple is type-stable and unrolled
    # per element, so each metric dispatches statically and the result tuple's
    # slots are concretely typed. The previous `ntuple(...) do i; ms.metrics[i]`
    # indexed the heterogeneous tuple with a runtime index, which is
    # type-unstable and boxed both the indexed metric and the returned values on
    # every call -- a top per-task allocation source.
    start_values = map(ms.metrics) do m
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
        # Stop each metric with its paired start value. Metric stop functions are
        # mutually independent (each measures an absolute quantity or a delta
        # against its own start), so unlike a nested resource stack the stop
        # order is irrelevant; a forward `map` over both tuples stays type-stable.
        final_values = map(ms.metrics, start_values) do m, start_value
            if metric_applies(m, ctx_val)
                if is_result_metric(m) && result !== nothing
                    return result_metric(m, result)
                else
                    return stop_metric(m, start_value)
                end
            else
                return nothing
            end
        end
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
                       key::K, values::Tuple) where K
    journal = get_journal(cache)
    bulk_update!(cache) do c
        ctx = pending_context!(c, mod, context, K)
        _apply_metric_values!(ctx, journal, mod, context, key, ms.metrics, values)
    end
    return
end

# Type-stable recursion over the (heterogeneous) metrics/values tuples. Handling
# each element with its concrete type avoids the boxing that a runtime-indexed
# `values[i]`/`ms.metrics[i]` loop incurred on the commit hot path (the same
# fix as in `with_metrics`).
@inline _apply_metric_values!(ctx, journal, mod, context, key, ::Tuple{}, ::Tuple{}) = nothing
@inline function _apply_metric_values!(ctx, journal, mod, context, key,
                                       metrics::Tuple, values::Tuple)
    m = first(metrics)
    v = first(values)
    if v !== nothing
        storage = get_or_create_storage!(ctx, m)
        set_metric_value!(storage, key, v)
        if journal !== nothing
            append_journal!(journal, (mod, context), m, key, v)
        end
    end
    _apply_metric_values!(ctx, journal, mod, context, key, Base.tail(metrics), Base.tail(values))
    return nothing
end
