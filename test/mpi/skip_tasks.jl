# Dagger.skip_tasks!: a rank that does not take part in a task leaves the @spawn out; ids stay uniform
const SKIP_SEEN = Int[]
const SKIP_LOCK = ReentrantLock()
record_skip(k) = (lock(() -> push!(SKIP_SEEN, k), SKIP_LOCK); k)
@testset "skip_tasks!: ids stay uniform when ranks skip foreign tasks" begin
    nr = MPI.Comm_size(comm); me = MPI.Comm_rank(comm)
    own_task(x) = x + 1
    old = Dagger.ELIDE_FOREIGN_TASKS[]
    # Uniformity checks are collective over all ranks and so disable elision (and with it skipping)
    Dagger.check_uniformity!(false)
    Dagger.elide_foreign_tasks!(true)
    try
        # No warm-up: the first thing a rank does may be to skip (starting the eager scheduler draws an id, see skip_tasks!)
        # ASYMMETRIC on purpose: rank 0 takes part in all N tasks, the others in none (a symmetric batch hides counter drift).
        # No fetch of a skipped task: a fetch is a collective (the owner sends the value to every rank) and draws ids.
        N = 7
        empty!(SKIP_SEEN)
        ts = Any[]
        for k in 1:N
            if me == 0
                push!(ts, Dagger.@spawn scope=Dagger.scope(mpi_rank=0) return_type=Int record_skip(k))
            else
                Dagger.skip_tasks!(1)
            end
        end
        if me == 0
            t0 = time(); while length(SKIP_SEEN) < N && time() - t0 < 30; sleep(0.01); end      # a failure-only task is not waited for by `wait`
            @test sort(SKIP_SEEN) == collect(1:N)
        end
        # the next task of the shared counter has the same id on every rank, past the batch
        nxt = Dagger.@spawn scope=Dagger.scope(mpi_rank=0) return_type=Int own_task(0)
        uids = MPI.Allgather([Int(nxt.uid)], comm)
        @test allequal(uids)
        @test fetch(nxt) == 1
        if me == 0
            # consecutive: nothing else drew from the counter (the first task of the process is followed by the id the eager scheduler takes)
            mine = [Int(t.uid) for t in ts]
            @test mine[2:end] == collect(mine[2]:mine[2] + N - 2)
        end
        # several at once, none, and a negative count
        Dagger.skip_tasks!(0)
        @test_throws ArgumentError Dagger.skip_tasks!(-1)
    finally
        Dagger.elide_foreign_tasks!(old)
        Dagger.check_uniformity!(true)
    end
    @test_throws ArgumentError (Dagger.elide_foreign_tasks!(false); try Dagger.skip_tasks!(1) finally Dagger.elide_foreign_tasks!(old) end)
end
