"""
    TableStorage

LogWindow-compatible aggregator which stores logs in a Tables.jl-compatible
sink.

Using a `TableStorage` is reasonably simple:

```julia
ml = Dagger.MultiEventLog()

... # Add some events

lw = Dagger.Events.LogWindow(5*10^9, :core)

# Create a DataFrame with one Any[] for each event
df = DataFrame([key=>[] for key in keys(ml.consumers)]...)

# Create the TableStorage and register its creation handler
ts = Dagger.Events.TableStorage(df)
push!(lw.creation_handlers, ts)

ml.aggregators[:lw] = lw

# Logs will now be saved into `df` automatically, and packages like
# DaggerWebDash.jl will automatically use it to retrieve subsets of the logs.
```
"""
struct TableStorage{T}
    sink::T
    function TableStorage(sink::T) where T
        @assert Tables.istable(sink)
        new{T}(sink)
    end
end
Dagger.Events.init_similar(ts::TableStorage) = TableStorage(similar(ts.sink, 0))
function Dagger.Events.creation_hook(ts::TableStorage, log)
    try
        push!(ts.sink, NamedTuple(log))
    catch err
        rethrow(err)
    end
end
function Base.getindex(ts::TableStorage, ts_range::UnitRange)
    ts_low, ts_high = ts_range.start, ts_range.stop
    return filter(row->ts_low <= row.core.timestamp <= ts_high,
                  Tables.rows(ts.sink)) |> Tables.materializer(ts.sink)
end
