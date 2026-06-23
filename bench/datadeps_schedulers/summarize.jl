using Statistics
using Printf

struct CSVRow
    workload::String
    scheduler::String
    tile_count::Int
    block_size::Int
    trial::Int
    total_wallclock_ms::Float64
    sched_phase_ms::Float64
    exec_span_ms::Float64
    n_tasks::Int
    n_copies::Int
    copy_total_ms::Float64
    compute_total_ms::Float64
    move_total_ms::Float64
    metrics_warm::Bool
end

function _parse_bool(s)
    sl = lowercase(strip(s))
    sl in ("true", "1", "t", "yes") && return true
    sl in ("false", "0", "f", "no") && return false
    error("Cannot parse bool from $(repr(s))")
end

function read_csv(path::AbstractString)
    rows = CSVRow[]
    open(path, "r") do io
        header = readline(io)
        # Tolerate CSVs predating the metrics_warm column.
        cols = split(header, ',')
        has_warm = "metrics_warm" in cols
        for line in eachline(io)
            isempty(strip(line)) && continue
            f = split(line, ',')
            warm = has_warm ? _parse_bool(f[14]) : false
            push!(rows, CSVRow(
                String(f[1]), String(f[2]),
                parse(Int, f[3]), parse(Int, f[4]), parse(Int, f[5]),
                parse(Float64, f[6]), parse(Float64, f[7]), parse(Float64, f[8]),
                parse(Int, f[9]), parse(Int, f[10]),
                parse(Float64, f[11]), parse(Float64, f[12]), parse(Float64, f[13]),
                warm,
            ))
        end
    end
    return rows
end

function _group_by(rows::Vector{CSVRow})
    groups = Dict{Tuple{String, Int, String, Bool}, Vector{CSVRow}}()
    for r in rows
        key = (r.workload, r.tile_count, r.scheduler, r.metrics_warm)
        push!(get!(groups, key, CSVRow[]), r)
    end
    return groups
end

function summarize_to_markdown(csv_path::AbstractString, md_path::AbstractString)
    rows = read_csv(csv_path)
    isempty(rows) && error("No rows in $csv_path")

    groups = _group_by(rows)
    keys_sorted = sort(collect(keys(groups));
                       by = k -> (k[1], k[2], k[3], k[4]))

    any_warm = any(r.metrics_warm for r in rows)

    open(md_path, "w") do io
        println(io, "# Datadeps scheduler benchmark — median across trials")
        println(io)
        println(io, "Source CSV: `$(basename(csv_path))` · $(length(rows)) trials across $(length(groups)) cells")
        any_warm && println(io, "\n_Some rows use a pre-warmed `MetricsTracker` cache (column `warm`)._")
        println(io)

        println(io, "| workload | nt | scheduler | warm | total (ms) | sched (ms) | exec (ms) | tasks | copies |")
        println(io, "|----------|----|-----------|------|-----------:|-----------:|----------:|------:|-------:|")
        for k in keys_sorted
            rs = groups[k]
            med_total = median(r.total_wallclock_ms for r in rs)
            med_sched = median(r.sched_phase_ms for r in rs)
            med_exec  = median(r.exec_span_ms for r in rs)
            n_tasks   = rs[1].n_tasks
            n_copies  = rs[1].n_copies
            workload, nt, sched, warm = k
            @printf(io, "| %s | %d | %s | %s | %.3f | %.3f | %.3f | %d | %d |\n",
                    workload, nt, sched, warm ? "yes" : "no",
                    med_total, med_sched, med_exec, n_tasks, n_copies)
        end
        println(io)

        # Ratio table vs RoundRobinScheduler baseline; one column per other scheduler.
        baseline = "RoundRobinScheduler"
        all_schedulers = sort(unique(r.scheduler for r in rows))
        others = filter(!=(baseline), all_schedulers)
        if !isempty(others) && baseline in all_schedulers
            println(io, "## Total-wallclock ratio vs $baseline (median)")
            println(io)
            println(io, "Ratio < 1.0 ⇒ scheduler beat $baseline. AOT scheduling overhead is included in `total`.")
            println(io)
            header_cols = String["workload", "nt", "warm", "$baseline (ms)"]
            for s in others
                push!(header_cols, "$s (ms)", "$s / RR")
            end
            println(io, "| " * join(header_cols, " | ") * " |")
            align = String["----------", "----", "------", "----------------:"]
            for _ in others
                push!(align, "---------------:", "---------:")
            end
            println(io, "| " * join(align, " | ") * " |")

            for workload in unique(r.workload for r in rows)
                for nt in sort(unique(r.tile_count for r in rows if r.workload == workload))
                    for warm in (false, true)
                        bl_key = (workload, nt, baseline, warm)
                        haskey(groups, bl_key) || continue
                        bl_med = median(r.total_wallclock_ms for r in groups[bl_key])
                        cells = String[workload, string(nt), warm ? "yes" : "no",
                                       @sprintf("%.3f", bl_med)]
                        any_other_data = false
                        for s in others
                            k = (workload, nt, s, warm)
                            if haskey(groups, k)
                                med = median(r.total_wallclock_ms for r in groups[k])
                                push!(cells, @sprintf("%.3f", med),
                                              @sprintf("%.3f", med / bl_med))
                                any_other_data = true
                            else
                                push!(cells, "—", "—")
                            end
                        end
                        any_other_data || continue
                        println(io, "| " * join(cells, " | ") * " |")
                    end
                end
            end
        end
    end
    return md_path
end

if abspath(PROGRAM_FILE) == @__FILE__
    if length(ARGS) != 2
        println(stderr, "Usage: julia summarize.jl <input.csv> <output.md>")
        exit(1)
    end
    summarize_to_markdown(ARGS[1], ARGS[2])
    println("Wrote $(ARGS[2])")
end
