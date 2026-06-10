path = ARGS[1]
start, finish = parse.(UInt64, ARGS[2:3])

using DataFrames, BenchmarkTools, Dagger, Serialization

logs = deserialize(path)["logs"]

"""
    spans(logs::DataFrame) -> DataFrame

Combines start and finish event pairs, and computes their timespan.
"""
function spans(logs)
    df = Any[]
    evs = Dict{Symbol,Dict{Any,Any}}()
    for log in eachrow(logs)
        _evs = get!(evs, log.core.category) do
            Dict{Any,Any}()
        end
        if log.core.kind == :finish
            if haskey(_evs, log.id)
                start = pop!(_evs, log.id)
                ev = merge(log, (;span=(start.core.timestamp, log.core.timestamp)))
                push!(df, ev)
            else
                @warn "Dropped :finish for $(log.core.category)"
            end
        elseif log.core.kind == :start
            _evs[log.id] = log
        end
    end
    DataFrame(df)
end

# Combine, select target range, and filter out `take`
tgt = subset(spans(logs), :core=>ByRow(x->start <= x.timestamp <= finish),
                          :core=>ByRow(x->x.category != :take))

# Sort by largest time contribution
tgt_sort = DataFrame(sort(eachrow(tgt), by=r->r.span[2] - r.span[1], rev=true))
transform!(tgt_sort, :span=>ByRow(s->(s[2]-s[1]) / (1000^3))=>:span_s)
println("Total: $((finish-start) / (1000^3))")
foreach(println, zip(map(c->c.category, tgt_sort.core), tgt_sort.span_s))
