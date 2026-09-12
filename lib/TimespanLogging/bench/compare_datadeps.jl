# Compare Datadeps matmul / cholesky: master vs this worktree, logging on vs off.
# Run with: timeout 600 julia --startup-file=no lib/TimespanLogging/bench/compare_datadeps.jl

const WORKTREE = abspath(joinpath(@__DIR__, "..", "..", ".."))
# Same Dagger commit as this worktree, old TimespanLogging, Manifest
# pinned to MemPool 0.4.18 (the live `Dagger` checkout's Manifest still
# has 0.4.15, which cannot load current Dagger).
const MASTER = get(ENV, "DAGGER_TSL_BASELINE", "/tmp/Dagger-tsl-baseline")
const WORKLOAD = joinpath(@__DIR__, "workload.jl")
const JULIA = Base.julia_cmd().exec[1]
const NTHREADS = "4"
const PROC_TIMEOUT = 240

function run_one(label::String, dagger_path::String, logging::Bool)
    isdir(dagger_path) || error("missing Dagger tree: $dagger_path")
    script = """
        include($(repr(WORKLOAD)))
        results = run_workload($(logging))
        print_results($(repr(label)), $(logging), results)
    """
    cmd = `timeout --kill-after=15 $(PROC_TIMEOUT) $(JULIA) --project=$(dagger_path) -t $(NTHREADS) --startup-file=no -e $(script)`
    println(stderr, "running $(label) logging=$(logging) ...")
    flush(stderr)
    return read(cmd, String)
end

function parse_results(text::String)
    rows = NamedTuple[]
    for line in split(text, '\n')
        startswith(line, "RESULT ") || continue
        fields = Dict{String,String}()
        for part in split(line[8:end], ' ')
            k, v = split(part, '='; limit=2)
            fields[k] = v
        end
        push!(rows, (
            tree=fields["tree"],
            logging=fields["logging"] == "true",
            op=Symbol(fields["op"]),
            time=parse(Float64, fields["time"]),
            allocs=parse(Int, fields["allocs"]),
            bytes=parse(Int, fields["bytes"]),
        ))
    end
    return rows
end

function fmt_bytes(b)
    b < 1024 && return "$(b) B"
    b < 1024^2 && return string(round(b / 1024; digits=1), " KiB")
    return string(round(b / 1024^2; digits=2), " MiB")
end

function main()
    configs = (
        ("master", MASTER, false),
        ("master", MASTER, true),
        ("worktree", WORKTREE, false),
        ("worktree", WORKTREE, true),
    )
    rows = NamedTuple[]
    for (label, path, logging) in configs
        try
            text = run_one(label, path, logging)
            print(text)
            append!(rows, parse_results(text))
        catch err
            @error "config failed" label logging exception=err
        end
    end
    println()
    println("Datadeps logging overhead (min of $(3) measured runs; N=512, B=64)")
    println(rpad("tree", 10), rpad("log", 6), rpad("op", 10),
            lpad("time (s)", 10), lpad("allocs", 12), lpad("bytes", 12))
    println(repeat("-", 60))
    for r in rows
        println(rpad(r.tree, 10), rpad(string(r.logging), 6), rpad(string(r.op), 10),
                lpad(string(round(r.time; digits=4)), 10),
                lpad(string(r.allocs), 12),
                lpad(fmt_bytes(r.bytes), 12))
    end
    println()
    println("logging-on / logging-off")
    for op in (:matmul, :cholesky)
        for tree in ("master", "worktree")
            off = findfirst(r -> r.tree == tree && !r.logging && r.op == op, rows)
            on = findfirst(r -> r.tree == tree && r.logging && r.op == op, rows)
            (off === nothing || on === nothing) && continue
            a, b = rows[off], rows[on]
            println(rpad(tree, 10), rpad(string(op), 10),
                    " time×", round(b.time / a.time; digits=2),
                    "  allocs×", round(b.allocs / a.allocs; digits=2),
                    "  bytes×", round(b.bytes / a.bytes; digits=2))
        end
    end
end

main()
