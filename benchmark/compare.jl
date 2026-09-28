# Shared by the CI driver and its tests; no package installation or benchmark run.
function compare_benchmarks(base, cur; threshold=0.10, alloc_threshold=0.25,
                            noise_tolerance=1.0)
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
            if key == "median" && noise_tolerance > 0
                # AirspeedVelocity omits quartiles for single-sample trials.
                # No measured spread is not evidence of a repeatable change.
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
