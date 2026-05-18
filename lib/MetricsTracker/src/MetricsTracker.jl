module MetricsTracker

import Serialization: serialize, deserialize
import ScopedValues: ScopedValue, @with
import TaskLocalValues: TaskLocalValue

export AbstractMetric, MetricsSpec, MetricsCache, MetricsSnapshot
export TimeMetric, ThreadTimeMetric, CompileTimeMetric, AllocMetric, ResultShapeMetric, LoadAverageMetric
export TransferTimeMetric, TransferSizeMetric, TransferRateMetric
export SyncTask, SyncGlobal, SyncInto
export LookupExact, LookupSubtype, LookupCustom
export @with_metrics, with_metrics
export snapshot, cache_lookup, find_keys, values_for_metric, lookup_value, metric_storage
export load_metrics!, save_metrics, attach_journal!, detach_journal!, compact_journal!
export metric_type, metric_applies, is_result_metric, start_metric, stop_metric, result_metric
export merge_into!, reset_global_cache!, global_metrics_cache, local_metrics_cache

include("types.jl")
include("cache.jl")
include("metrics.jl")
include("lookup.jl")
include("io.jl")
include("builtins.jl")

end
