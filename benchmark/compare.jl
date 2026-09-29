# Shared by the CI driver and its tests; no package installation or benchmark run.
function compare_benchmarks(base, cur; threshold=0.10, alloc_threshold=0.25,
                            noise_tolerance=1.0, min_samples=5)
    regressions = Tuple{String,String,Float64}[]
    improvements = Tuple{String,String,Float64}[]
    within_noise = Tuple{String,String,Float64}[]
    insufficient = Tuple{String,String,Float64}[]
    metrics = (("time", "median", threshold), ("allocs", "allocs", alloc_threshold),
               ("memory", "memory", alloc_threshold))
    for (name, stats) in cur
        name == "time_to_load" && continue
        haskey(base, name) || continue
        for (label, key, limit) in metrics
            bm = get(base[name], key, nothing)
            cm = get(stats, key, nothing)
            (bm === nothing || cm === nothing || bm == 0) && continue
            ratio = cm / bm
            regression = ratio > 1 + limit
            improvement = ratio < 1 - limit
            (regression || improvement) || continue
            if key == "median" &&
               (get(base[name], "samples", 0) < min_samples || get(stats, "samples", 0) < min_samples)
                push!(insufficient, (name, label, ratio))
                continue
            end
            if key == "median" && noise_tolerance > 0
                # Missing spread is not evidence of a repeatable change, even
                # when a caller supplies sufficient sample counts.
                if !all(s -> haskey(s, "25") && haskey(s, "75"), (base[name], stats))
                    push!(insufficient, (name, label, ratio))
                    continue
                end
                base_hw = noise_tolerance * (base[name]["75"] - base[name]["25"])
                cur_hw = noise_tolerance * (stats["75"] - stats["25"])
                significant = regression ? cm - cur_hw > bm + base_hw :
                                           cm + cur_hw < bm - base_hw
                if !significant
                    push!(within_noise, (name, label, ratio))
                    continue
                end
            end
            push!(regression ? regressions : improvements, (name, label, ratio))
        end
    end
    sort!(regressions; by=last, rev=true)
    sort!(improvements; by=last)
    sort!(within_noise; by=last, rev=true)
    sort!(insufficient; by=last, rev=true)
    return (; regressions, improvements, within_noise, insufficient)
end

# Airspeed's flattened statistics omit the number of measurements. Recover the
# actual count from each raw Trial, not params.samples (which is only a limit).
function attach_sample_counts!(flat, raw, prefix="")
    if haskey(raw, "times")
        haskey(flat, prefix) && (flat[prefix]["samples"] = length(raw["times"]))
    elseif haskey(raw, "data")
        for (key, value) in raw["data"]
            name = isempty(prefix) ? string(key) : prefix * "/" * string(key)
            attach_sample_counts!(flat, value, name)
        end
    end
    return flat
end

# Only timing regressions require a second independent comparison. Allocation
# metrics retain their existing gates; unconfirmed timing flags stay visible.
function confirm_timing_regressions(first, second)
    confirmed = Set((name, metric) for (name, metric, _) in second.regressions if metric == "time")
    regressions = filter(first.regressions) do (name, metric, _)
        metric != "time" || (name, metric) in confirmed
    end
    insufficient = copy(first.insufficient)
    for entry in first.regressions
        name, metric, _ = entry
        metric == "time" && !((name, metric) in confirmed) && push!(insufficient, entry)
    end
    sort!(insufficient; by=last, rev=true)
    return (; first..., regressions, insufficient)
end
