const TASK_SIGNATURE = ScopedValue{Union{Signature, Nothing}}(nothing)
struct SignatureMetric <: MT.AbstractMetric end
MT.metric_applies(::SignatureMetric, ::Val{:execute!}) = true
MT.metric_type(::SignatureMetric) = Union{Signature, Nothing}
MT.start_metric(::SignatureMetric) = nothing
MT.stop_metric(::SignatureMetric, _) = TASK_SIGNATURE[]

const TASK_PROCESSOR = ScopedValue{Union{Processor, Nothing}}(nothing)
struct ProcessorMetric <: MT.AbstractMetric end
MT.metric_applies(::ProcessorMetric, ::Val{:execute!}) = true
MT.metric_type(::ProcessorMetric) = Union{Processor, Nothing}
MT.start_metric(::ProcessorMetric) = nothing
MT.stop_metric(::ProcessorMetric, _) = TASK_PROCESSOR[]

# FIXME: struct TransferTimeMetric <: MT.AbstractMetric end
