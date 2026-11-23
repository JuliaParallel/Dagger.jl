# Task Bench task graphs: the measurements behind METG.
#
# The tasks suite times the runtime's cost per task with task bodies that do
# nothing. This suite measures what that cost means for a program: METG, the
# minimum effective task granularity of
#
#   Slaughter et al., "Task Bench: A Parameterized Benchmark for Evaluating
#   Parallel Runtime Performance", SC20. https://github.com/StanfordLegion/task-bench
#
# i.e. the smallest task for which the runtime still delivers a given fraction
# (50%) of the machine's peak FLOP/s. It runs Task Bench's own experiment
# (experiments/*/metg_*.sh): a task graph of `width` tasks per timestep, every
# task depending on tasks of the previous timestep, in which each task validates
# its inputs and runs the compute-bound kernel for `iter` iterations. `iter` is
# swept from large to small, one benchmark per value. One more benchmark runs a
# long kernel task on every processor at once: the FLOP/s reference. Several
# identical graphs can run at once, which gives every processor more than one
# task per timestep; Task Bench reports METG for one graph and for four.
#
# The trials are ordinary timings of whole graphs. `benchmark/metg.jl` runs this
# suite and derives the efficiency curve and METG from them; the BENCHMARK_METG_*
# variables are described in `benchmark/benchmarks.jl`.
#
# As in the tasks suite, placement is explicit (`ExactScope`) unless the
# benchmark is asked to be about placement, and timed runs `wait`. Since `wait`
# does not rethrow a failed task, every graph is probed with `fetch` before it
# is timed, and every timed run is followed by a `fetch` of its outputs, outside
# the clock.

@everywhere begin
    # A Task Bench task graph (core/core.h `TaskGraph`) with the compute-bound
    # kernel. Timesteps and points are 0-based, as in Task Bench.
    struct TBGraph
        dependence::Symbol
        timesteps::Int
        max_width::Int
        radix::Int      # dependencies per task (nearest, spread)
        period::Int     # dependence sets (spread)
        iterations::Int # kernel iterations per task
    end

    # The dependence types whose width is `max_width` at every timestep (tree,
    # dom and the random ones are not ported).
    const TB_DEPENDENCES = (:trivial, :no_comm, :stencil_1d, :stencil_1d_periodic,
                            :fft, :nearest, :spread, :all_to_all)

    # `TaskGraph::max_dependence_sets`
    function tb_max_dependence_sets(g::TBGraph)
        g.dependence === :fft && return max(1, ceil(Int, log2(g.max_width)))
        g.dependence === :spread && return g.period
        return 1
    end

    # `TaskGraph::dependence_set_at_timestep`
    function tb_dependence_set(g::TBGraph, t::Int)
        n = tb_max_dependence_sets(g)
        g.dependence === :fft && return (t + n - 1) % n
        g.dependence === :spread && return t % n
        return 0
    end

    # `TaskGraph::dependencies`: the points of timestep `t - 1` that point `p`
    # of timestep `t` reads, in Task Bench's order, with intervals expanded.
    function tb_dependencies!(deps::Vector{Int}, g::TBGraph, t::Int, p::Int)
        empty!(deps)
        t == 0 && return deps
        w = g.max_width
        dependence = g.dependence
        if dependence === :trivial
        elseif dependence === :no_comm
            push!(deps, p)
        elseif dependence === :stencil_1d
            append!(deps, max(0, p - 1):min(p + 1, w - 1))
        elseif dependence === :stencil_1d_periodic
            append!(deps, max(0, p - 1):min(p + 1, w - 1))
            p - 1 < 0 && push!(deps, w - 1)
            p + 1 >= w && push!(deps, 0)
        elseif dependence === :fft
            d = 1 << tb_dependence_set(g, t)
            p - d >= 0 && push!(deps, p - d)
            push!(deps, p)
            p + d < w && push!(deps, p + d)
        elseif dependence === :all_to_all
            append!(deps, 0:w-1)
        elseif dependence === :nearest
            g.radix > 0 && append!(deps, max(0, p - g.radix ÷ 2):min(p + (g.radix - 1) ÷ 2, w - 1))
        elseif dependence === :spread
            dset = tb_dependence_set(g, t)
            for i in 0:g.radix-1
                push!(deps, (p + i * w ÷ g.radix + (i > 0 ? dset : 0)) % w)
            end
        else
            error("unsupported dependence type $dependence")
        end
        return deps
    end

    # `execute_kernel_compute`: 64 doubles, `a = a * a + a` on each of them per
    # iteration, held in 16 four-wide vectors as on the kernel's AVX2 path.
    # `benchmark/metg.jl` credits a task with 2 * 64 * iterations + 64 FLOPs
    # (`count_flops_per_task`), so the loop has to run at the C kernel's speed
    # for the efficiency to be comparable. The FMA is therefore explicit:
    # written on tuples of scalars it is left to the auto-vectorizer, which
    # emits mostly two-wide FMAs: 18 GFLOP/s on a core where this version and
    # the C kernel both reach 31. Compare with the C kernel when touching it.
    const TBVec = NTuple{4,VecElement{Float64}}
    const TB_FMA_IR = ("""
        declare <4 x double> @llvm.fmuladd.v4f64(<4 x double>, <4 x double>, <4 x double>)
        define <4 x double> @entry(<4 x double> %a) #0 {
            %r = call <4 x double> @llvm.fmuladd.v4f64(<4 x double> %a, <4 x double> %a, <4 x double> %a)
            ret <4 x double> %r
        }
        attributes #0 = { alwaysinline }
        """, "entry")
    @inline tb_fma(a::TBVec) = Base.llvmcall(TB_FMA_IR, TBVec, Tuple{TBVec}, a)
    @inline tb_vec(a, b, c, d) = (VecElement(a), VecElement(b), VecElement(c), VecElement(d))
    @noinline function tb_kernel_compute(iterations::Int)
        Base.Cartesian.@nexprs 16 i -> a_i = tb_vec(4.0, 3.0, 2.0, 1.0)
        for _ in 1:iterations
            Base.Cartesian.@nexprs 16 i -> a_i = tb_fma(a_i)
        end
        dot = 1.0
        Base.Cartesian.@nexprs 16 i -> dot = dot * a_i[1].value * a_i[2].value * a_i[3].value * a_i[4].value
        return dot
    end

    # `TaskGraph::execute_point`: validate every input against the dependence
    # pattern, run the kernel, produce the output: one (timestep, point) pair,
    # Task Bench's default of 16 bytes.
    function tb_execute_point(g::TBGraph, t::Int, p::Int, inputs::Vararg{Tuple{Int,Int},N}) where N
        deps = tb_dependencies!(Int[], g, t, p)
        length(deps) == N || error("task ($t, $p): got $N inputs, expected $(length(deps))")
        for i in 1:N
            inputs[i] == (t - 1, deps[i]) ||
                error("task ($t, $p): input $i is $(inputs[i]), expected $((t - 1, deps[i]))")
        end
        Base.donotdelete(tb_kernel_compute(g.iterations))
        return (t, p)
    end
end

const metg_types = Symbol.(strip.(split(get(ENV, "BENCHMARK_METG_TYPE", "stencil_1d"), ',')))
const metg_steps = parse(Int, get(ENV, "BENCHMARK_METG_STEPS", "1000"))
const metg_width = parse(Int, get(ENV, "BENCHMARK_METG_WIDTH", "0"))
const metg_radix = parse(Int, get(ENV, "BENCHMARK_METG_RADIX", "5"))
const metg_ngraphs = parse.(Int, strip.(split(get(ENV, "BENCHMARK_METG_NGRAPHS", "1"), ',')))
const metg_exponents = let parsed = eval(Meta.parse(get(ENV, "BENCHMARK_METG_ITER", "8:26")))
    sort!(unique!(Int[parsed...]))
end
const metg_placement = get(ENV, "BENCHMARK_METG_PLACEMENT", "pinned")
const metg_launch_mode = get(ENV, "BENCHMARK_METG_LAUNCH", "task")

# Task Bench's default period for the spread pattern, which the METG experiments
# do not override
const TB_SPREAD_PERIOD = 3

function metg_graph(dependence::Symbol, timesteps, max_width, radix, iterations)
    dependence in TB_DEPENDENCES ||
        error("BENCHMARK_METG_TYPE: unknown dependence type `$dependence` (supported: $(join(TB_DEPENDENCES, ", ")))")
    timesteps >= 1 && max_width >= 1 || error("The metg suite needs at least one timestep and one task per timestep")
    period = 0
    if dependence === :spread
        period = TB_SPREAD_PERIOD
        # `App::check`: avoids wrapping around with later dependence sets
        radix >= 1 && period <= cld(max_width, radix) ||
            error("The spread pattern needs BENCHMARK_METG_RADIX >= 1 and a width of more than $((period - 1) * radix) for that radix (got radix $radix, width $max_width)")
    elseif dependence === :nearest
        radix >= 0 || error("BENCHMARK_METG_RADIX must not be negative")
    end
    return TBGraph(dependence, timesteps, max_width, radix, period, iterations)
end

# Group key of a graph's sweep. `benchmark/metg.jl` parses it.
function metg_label(g::TBGraph, ngraphs, placement, launch)
    label = String(g.dependence)
    g.dependence in (:nearest, :spread) && (label *= " radix=$(g.radix)")
    ngraphs > 1 && (label *= " graphs=$ngraphs")
    label *= " steps=$(g.timesteps) width=$(g.max_width) $placement"
    launch == "bulk" && (label *= " bulk")
    return label
end

metg_spawn(g, t, p, inputs, scopes::Vector{Dagger.ExactScope}) =
    Dagger.@spawn scope=scopes[p + 1] tb_execute_point(g, t, p, inputs...)
metg_spawn(g, t, p, inputs, ::Nothing) =
    Dagger.@spawn tb_execute_point(g, t, p, inputs...)

# Launches timestep `t` of one graph, whose point `p` reads `prev[d + 1]` for
# every dependency `d`
function metg_launch_row(g::TBGraph, t::Int, prev::Vector{Dagger.DTask}, scopes)
    row = Vector{Dagger.DTask}(undef, g.max_width)
    deps = Int[]
    inputs = Dagger.DTask[]
    for p in 0:g.max_width-1
        tb_dependencies!(deps, g, t, p)
        empty!(inputs)
        for d in deps
            push!(inputs, prev[d + 1])
        end
        row[p + 1] = metg_spawn(g, t, p, inputs, scopes)
    end
    return row
end
# Launches timestep `t` of every graph, `prev` holding each graph's previous
# timestep. The graphs are independent copies of `g` and advance together, as in
# Task Bench's Legion implementation.
metg_launch_step(g::TBGraph, t::Int, prev::Vector{Vector{Dagger.DTask}}, scopes) =
    Vector{Dagger.DTask}[metg_launch_row(g, t, row, scopes) for row in prev]
# The same, handing the timestep to the scheduler as one batch
metg_launch_step_bulk(g::TBGraph, t::Int, prev::Vector{Vector{Dagger.DTask}}, scopes) =
    Dagger.spawn_bulk(() -> metg_launch_step(g, t, prev, scopes))::Vector{Vector{Dagger.DTask}}

# Launches `ngraphs` graphs and returns the tasks that nothing depends on. Only
# those are kept: earlier timesteps are released as the graphs advance.
function metg_launch(g::TBGraph, ngraphs::Int, scopes, bulk::Bool)
    # Every pattern with dependencies has each point read its own column, so the
    # last timestep transitively depends on all tasks; without dependencies no
    # task depends on any other
    independent = isempty(tb_dependencies!(Int[], g, 1, 0))
    sinks = Dagger.DTask[]
    prev = [Dagger.DTask[] for _ in 1:ngraphs]
    for t in 0:g.timesteps-1
        prev = bulk ? metg_launch_step_bulk(g, t, prev, scopes) : metg_launch_step(g, t, prev, scopes)
        if independent || t == g.timesteps - 1
            for row in prev
                append!(sinks, row)
            end
        end
    end
    return sinks
end

# The timed operation. Like Task Bench's clock, it covers launching the graphs
# as well as running them.
function metg_run!(out::Base.RefValue{Vector{Dagger.DTask}}, g::TBGraph, ngraphs::Int, scopes, bulk::Bool)
    sinks = metg_launch(g, ngraphs, scopes, bulk)
    foreach(wait, sinks)
    out[] = sinks
    return nothing
end

# Task Bench validates every run: the inputs inside each task (a failure there
# fails every task downstream), the final outputs here.
function metg_check(g::TBGraph, ngraphs::Int, sinks::Vector{Dagger.DTask})
    first_step = g.timesteps - length(sinks) ÷ (ngraphs * g.max_width)
    expected = [(t, p) for t in first_step:g.timesteps-1 for _ in 1:ngraphs for p in 0:g.max_width-1]
    fetch.(sinks) == expected || error("Task Bench graph ($(g.dependence)) produced wrong output")
    return nothing
end

# The FLOP/s reference: one kernel task on every processor, all at once
function metg_peak_run!(out::Base.RefValue{Vector{Dagger.DTask}}, procs, iterations::Int)
    tasks = Dagger.DTask[Dagger.@spawn(scope=Dagger.ExactScope(proc), tb_kernel_compute(iterations)) for proc in procs]
    foreach(wait, tasks)
    out[] = tasks
    return nothing
end

function metg_suite(_ctx; method, accels)
    @assert method == "dagger" "The metg suite only supports `dagger` execution"
    isempty(accels) || error("The metg suite is CPU-only")
    metg_placement in ("pinned", "unscoped") || error("BENCHMARK_METG_PLACEMENT must be `pinned` or `unscoped`")
    metg_launch_mode in ("task", "bulk") || error("BENCHMARK_METG_LAUNCH must be `task` or `bulk`")
    !isempty(metg_exponents) && all(e -> 0 <= e <= 60, metg_exponents) ||
        error("BENCHMARK_METG_ITER must select exponents of two between 0 and 60")
    all(>=(1), metg_ngraphs) || error("BENCHMARK_METG_NGRAPHS must be positive")

    # CPU processors in the tasks suite's order: identical on every rank, with
    # every owner's (worker's / rank's) processors contiguous
    accel = Dagger.current_acceleration()
    procs = collect(Dagger.compatible_processors(accel))
    sort!(procs; by=p -> Dagger.processor_order_key(accel, p))
    owners = length(unique!([Dagger.processor_owner_rank(accel, p) for p in procs]))

    # Task Bench's METG experiments give every core one column of the graph
    width = metg_width > 0 ? metg_width : length(procs)
    # Columns map to processors in contiguous blocks, as Task Bench's MPI
    # implementation maps them to ranks
    scopes = metg_placement == "pinned" ?
        Dagger.ExactScope[Dagger.ExactScope(procs[p * length(procs) ÷ width + 1]) for p in 0:width-1] : nothing
    bulk = metg_launch_mode == "bulk"

    sub = BenchmarkGroup()

    # Long enough for the runtime's cost to vanish next to the largest task of the sweep
    peak_exponent = maximum(metg_exponents) + 2
    if supported(() -> (out = Ref{Vector{Dagger.DTask}}(); metg_peak_run!(out, procs, 1); foreach(fetch, out[])), "metg/peak")
        sub["peak"] = BenchmarkGroup()
        sub["peak"]["iter=2^$(lpad(peak_exponent, 2, '0'))"] = @benchmarkable(
            metg_peak_run!(out, $procs, $(1 << peak_exponent)),
            setup = (out = Ref{Vector{Dagger.DTask}}()),
            teardown = (foreach(fetch, out[]); out = nothing; @everywhere GC.gc()))
    end

    for dependence in metg_types, ngraphs in metg_ngraphs
        graph(iterations; timesteps=metg_steps) = metg_graph(dependence, timesteps, width, metg_radix, iterations)
        label = metg_label(graph(1), ngraphs, metg_placement, metg_launch_mode)
        # A few timesteps at a tiny size reach every dependence set
        probe = graph(1; timesteps=min(metg_steps, tb_max_dependence_sets(graph(1)) + 2))
        supported(() -> (out = Ref{Vector{Dagger.DTask}}(); metg_run!(out, probe, ngraphs, scopes, bulk);
                         metg_check(probe, ngraphs, out[])), "metg/$label") || continue
        sweep = BenchmarkGroup()
        for exponent in metg_exponents
            g = graph(1 << exponent)
            sweep["iter=2^$(lpad(exponent, 2, '0'))"] = @benchmarkable(
                metg_run!(out, $g, $ngraphs, $scopes, $bulk),
                setup = (out = Ref{Vector{Dagger.DTask}}()),
                teardown = (metg_check($g, $ngraphs, out[]); out = nothing; @everywhere GC.gc()))
        end
        sub[label] = sweep
    end

    suite = BenchmarkGroup()
    suite["owners=$owners procs=$(length(procs))"] = sub
    return suite
end

return metg_suite
