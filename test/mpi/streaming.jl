# A uniform region of more than DATADEPS_UNIFORM_DEFER_MAX tasks is streamed through the
# asynchronous submitter instead of being withheld until it is fully planned. The result must
# match the sequential one, and a failure inside the region must still reach every rank.
stream_add!(x, k) = (x .+= k; nothing)
stream_boom!(x) = error("boom in streamed region")
include("deadline.jl")

# Counts the tasks of the region that have started in this process
const STREAM_STARTED = Threads.Atomic{Int}(0)
stream_count!(x) = (Threads.atomic_add!(STREAM_STARTED, 1); x .+= 1; nothing)

# Round-robin placement that also reports, when the planner places the last task of the region,
# whether any task of the region has started by then in this process. A region that is withheld until
# it is fully planned has started nothing; a streamed one has been running for most of the planning.
# (It does not declare itself stateless, so it never takes the windowed path.)
mutable struct StreamProbe <: Dagger.DataDepsScheduler
    inner::Dagger.RoundRobinScheduler
    calls::Threads.Atomic{Int}
    last::Int
    started_before_last::Base.RefValue{Union{Nothing,Bool}}
    wait_secs::Float64
end
StreamProbe(last::Int, wait_secs) = StreamProbe(Dagger.RoundRobinScheduler(), Threads.Atomic{Int}(0), last,
                                                Ref{Union{Nothing,Bool}}(nothing), wait_secs)
Base.similar(p::StreamProbe) = StreamProbe(similar(p.inner), p.calls, p.last, p.started_before_last, p.wait_secs)
function Dagger.datadeps_schedule_task(p::StreamProbe, args...)
    k = Threads.atomic_add!(p.calls, 1) + 1
    if k == p.last
        before = STREAM_STARTED[]
        p.started_before_last[] = timedwait(() -> STREAM_STARTED[] > before, p.wait_secs; pollint=0.005) === :ok ||
                                  before > 0
    end
    return Dagger.datadeps_schedule_task(p.inner, args...)
end

function stream_probe_region(ntasks; wait_secs)
    probe = StreamProbe(ntasks, wait_secs)
    STREAM_STARTED[] = 0
    # arrays on every rank, so that every rank has tasks of its own to start
    arrs = [Dagger.tochunk(zeros(4), proc_for_rank(mod(i, nranks)), space_for_rank(mod(i, nranks))) for i in 1:ntasks]
    within(120, "probe region of $ntasks tasks") do
        Dagger.spawn_datadeps(; scheduler=probe) do
            for i in 1:ntasks
                Dagger.spawn(stream_count!, InOut(arrs[i]))
            end
        end
    end
    @test probe.calls[] == ntasks
    @test all(a -> fetch(a) == fill(1.0, 4), arrs)
    return probe.started_before_last[]
end

@testset "Streaming starts tasks while the region is still being planned" begin
    # up to the limit the region is withheld until planned: nothing has started when the last task is placed
    @test stream_probe_region(Dagger.DATADEPS_UNIFORM_DEFER_MAX; wait_secs=0.5) === false
    # above it, tasks run while the rest is planned (with one thread per rank the submitter hands
    # batches over in place, which starts tasks as well)
    @test stream_probe_region(Dagger.DATADEPS_UNIFORM_DEFER_MAX + 1; wait_secs=30.0) === true
end

@testset "Streamed uniform regions" begin
    ntasks = Dagger.DATADEPS_UNIFORM_DEFER_MAX + 88
    narr = 4
    # N.B. `hierarchical=false` takes the flat planner, which never streams under MPI; it only checks
    # that both planners agree on the result
    @testset "hierarchical=$hierarchical" for hierarchical in (false, true)
        arrs = [Dagger.tochunk(zeros(4)) for _ in 1:narr]
        Dagger.spawn_datadeps(; hierarchical) do
            for i in 1:ntasks
                Dagger.spawn(stream_add!, InOut(arrs[mod1(i, narr)]), i)
            end
        end
        for j in 1:narr
            want = sum(i for i in 1:ntasks if mod1(i, narr) == j)
            @test fetch(arrs[j]) == fill(float(want), 4)
        end
    end
    @testset "a failing task reaches the caller" begin
        A = Dagger.tochunk(zeros(4))
        B = Dagger.tochunk(zeros(4))
        threw = false
        try
            Dagger.spawn_datadeps() do
                for i in 1:ntasks
                    Dagger.spawn(stream_add!, InOut(A), 1)
                    i == ntasks ÷ 2 && Dagger.spawn(stream_boom!, InOut(B))
                end
            end
        catch err
            threw = true
            @test occursin("boom in streamed region", sprint(showerror, err))
        end
        @test threw
    end
end

stream_cross_add!(x, y, k) = (x .+= k .* y; nothing)
stream_cross_bump!(x, s) = (x .+= s; nothing)

# Arguments owned by different ranks and tasks that consume an earlier task's value (these flush the
# submitter), at the limit and just above it
@testset "Streamed region, cross-rank and value dependencies" begin
    for ntasks in (Dagger.DATADEPS_UNIFORM_DEFER_MAX, Dagger.DATADEPS_UNIFORM_DEFER_MAX + 1,
                   Dagger.DATADEPS_UNIFORM_DEFER_MAX + 188)
        narr = 2 * nranks
        arrs = [Dagger.tochunk(fill(float(j), 4), proc_for_rank(mod(j, nranks)),
                               space_for_rank(mod(j, nranks))) for j in 1:narr]
        plan = [(rand(MersenneTwister(i), 1:3), mod1(i, narr), mod1(i + 1 + (i % 3), narr)) for i in 1:ntasks]
        Dagger.spawn_datadeps() do
            for (i, (kind, a, b)) in enumerate(plan)
                a == b && continue
                if kind == 3
                    v = Dagger.spawn(sum, In(arrs[b]))
                    Dagger.spawn(stream_cross_bump!, InOut(arrs[a]), v)
                else
                    Dagger.spawn(stream_cross_add!, InOut(arrs[a]), In(arrs[b]), i)
                end
            end
        end
        vals = [fill(float(j), 4) for j in 1:narr]
        for (i, (kind, a, b)) in enumerate(plan)
            a == b && continue
            if kind == 3
                vals[a] .+= sum(vals[b])
            else
                vals[a] .+= i .* vals[b]
            end
        end
        for j in 1:narr
            @test fetch(arrs[j]) == vals[j]
        end
    end
end
