# Compare this script across revisions using the same Julia environment.
# Use an environment containing Dagger and MPI.
# Example: julia --project=/path/to/env test/mpi/run.jl 2 2 benchmark/launch_order.jl
# Seven samples follow ten warmups; GC and the cooperative MPI barrier are
# outside the timed region. Wall times are the maximum across ranks, and GC
# counts are reported separately for each rank. Uniformity checks default off.
# DAGGER_LAUNCH_BENCH_CASE selects one case; DAGGER_LAUNCH_BENCH_SAMPLES must be odd.
# DAGGER_LAUNCH_BENCH_COPIES=true counts planned move! tasks in one untimed call.
using Dagger, MPI
Dagger.accelerate!(:mpi)
const uniformity_checks = parse(Bool, get(ENV, "DAGGER_LAUNCH_BENCH_CHECKS", "false"))
Dagger.check_uniformity!(uniformity_checks)
const samples = parse(Int, get(ENV, "DAGGER_LAUNCH_BENCH_SAMPLES", "7"))
samples > 0 && isodd(samples) || error("Sample count must be positive and odd")
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
function barrier()
    req = MPI.Ibarrier(comm)
    while !MPI.Test(req)
        sleep(0.001)
    end
end
function work(i, n)
    s = 0.0
    for j in 1:n
        x = (j - 0.5) / n
        s += 4.0 / (1.0 + x*x)
    end
    i*s/n
end
function scalar_tree()
    t = Dagger.spawn_datadeps() do
        p = [Dagger.@spawn work(i, 1<<24) for i in 1:16]
        m = length(p)
        while m>1
            for k in 1:m÷2
                p[k] = Dagger.@spawn +(p[2k-1], p[2k])
            end
            m÷=2
        end
        p[1]
    end
    fetch(t)
end
function fast_tree()
    t = Dagger.spawn_datadeps() do
        p = [Dagger.@spawn identity(i) for i in 1:64]
        Dagger.treereduce((x,y)->Dagger.@spawn(+(x,y)), p)
    end
    fetch(t)
end
noop!(A) = nothing
const buffers = [zeros(32) for _ in 1:32]
function buffer_region()
    Dagger.spawn_datadeps() do
        for A in buffers
            Dagger.@spawn noop!(Dagger.InOut(A))
        end
    end
end
const source = reshape(collect(Float64,1:512*512),512,512)
const fixture = Dagger.DArray(source, Dagger.Blocks(64,64))
wait(fixture)
const cases = (("scalar_tree",scalar_tree,x->isapprox(x,136pi;rtol=1e-6)),
               ("fast_tree",fast_tree,x->x==sum(1:64)),
               ("collect",()->collect(fixture),x->x==source),
               ("buffer_region",buffer_region,x->true))
rank == 0 && println("Julia ", VERSION, ", ranks=", MPI.Comm_size(comm),
                     ", threads/rank=", Threads.nthreads(),
                     ", uniformity_checks=", uniformity_checks, ", samples=", samples)
selected = get(ENV, "DAGGER_LAUNCH_BENCH_CASE", "")
isempty(selected) || any(case -> first(case) == selected, cases) ||
    error("Unknown launch-order benchmark case: $selected")
for (name,f,check) in cases
    isempty(selected) || selected == name || continue
    for _ in 1:10
        @assert check(f())
    end
    GC.gc(); barrier()
    times = Float64[]; allocs=Int[]; bytes=Int[]
    for _ in 1:samples
        GC.gc(); barrier()
        before = Base.gc_num()
        start = time_ns()
        result=f()
        elapsed=(time_ns()-start)/1e9
        diff=Base.GC_Diff(Base.gc_num(),before)
        @assert check(result)
        push!(times,MPI.Allreduce(elapsed,max,comm))
        push!(allocs,Base.gc_alloc_count(diff)); push!(bytes,Int(diff.allocd))
    end
    move_counts = nothing
    if parse(Bool, get(ENV, "DAGGER_LAUNCH_BENCH_COPIES", "false"))
        Dagger.enable_logging!(; tasknames=true, all_task_deps=false)
        count = 0
        try
            @assert check(f())
            logs = Dagger.fetch_logs!()
            for worker_logs in values(logs), taskname in worker_logs[:tasknames]
                if taskname isa String && (occursin("move!", taskname) ||
                                           occursin("move_toplevel!", taskname))
                    count += 1
                end
            end
        finally
            Dagger.disable_logging!()
        end
        move_counts = MPI.gather(count, comm)
    end
    all_allocs=MPI.gather(minimum(allocs),comm)
    all_bytes=MPI.gather(minimum(bytes),comm)
    if rank==0
        ordered=sort(times)
        println(name," median_s=",ordered[cld(samples, 2)]," minimum_s=",first(ordered)," allocs_by_rank=",all_allocs," bytes_by_rank=",all_bytes," planned_moves_by_rank=",move_counts," samples=",times)
    end
end
