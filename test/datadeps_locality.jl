# Locality-aware placement (`DATADEPS_LOCALITY_BIAS`, `src/datadeps/scheduling.jl`).
#
# White-box: constructs a `DataDepsState`/`ArgumentWrapper` directly and hand-sets
# `arg_current`, then drives `RoundRobinScheduler`'s `datadeps_schedule_task`
# in a loop the way `distribute_task!` would -- one call per synthetic task,
# reusing the same scheduler instance so its round-robin rotation state
# persists across calls, exactly as within one real region.
#
# This bypasses `spawn_datadeps`'s copy/aliasing pipeline (whose read/copy
# semantics make it hard to control exactly which spaces end up "current")
# entirely, which is also why it needs no `Dagger.@spawn`d work to actually
# execute: `RoundRobinScheduler` never touches its `task::DTask` argument, and
# `proc_in_scope`/`ExactScope` only need their candidate processors to
# correspond to *live* worker processes (a liveness `remotecall_fetch`), not
# to have run anything.
import Dagger: DataDepsState, ArgumentWrapper, DTaskSpec, Argument,
               RoundRobinScheduler, datadeps_schedule_task, In,
               CPURAMMemorySpace, Options, UnionScope, ExactScope

@testset "DATADEPS_LOCALITY_BIAS" begin
    if nprocs() < 4
        @test_skip "Needs 4 processes (-p 3) for a 4-memory-space scenario"
    else
        all_procs = [only(Dagger.get_processors(Dagger.OSProc(pid))) for pid in 1:4]
        all_scope = UnionScope(map(ExactScope, all_procs))
        task_scope = all_scope

        # Two tracked arguments with sizes chosen so their locality fractions
        # are 1.0 and 0.5 (not just all-or-nothing): the fully-resident one
        # sits at space 4 (rotation offset 3, the *farthest* point from the
        # scheduler's starting position), the half-resident one at space 1
        # (offset 0, the *closest*). This is the shape that makes `bias=0.5`
        # (weighs distance against locality) diverge from `bias=1.0` (ignores
        # distance entirely) despite both preferring *some* locality over
        # none -- a single all-or-nothing weight can't distinguish them, since
        # (with this scoring's bounded staleness term) `bias=0.5` already
        # always prefers full locality regardless of distance.
        raw_big = fill(0.0, 10_000)   # resident ONLY at space 4, locality 1.0
        raw_small = fill(0.0, 5_000)  # resident ONLY at space 1, locality 0.5

        state = DataDepsState()
        chunk_big = Dagger.tochunk(raw_big)
        chunk_small = Dagger.tochunk(raw_small)
        state.raw_arg_to_chunk[raw_big] = chunk_big
        state.raw_arg_to_chunk[raw_small] = chunk_small
        arg_w_big = ArgumentWrapper(chunk_big, identity)
        arg_w_small = ArgumentWrapper(chunk_small, identity)
        state.arg_current[arg_w_big] = Set([CPURAMMemorySpace(4)])
        state.arg_current[arg_w_small] = Set([CPURAMMemorySpace(1)])

        task = Dagger.@spawn 1 + 1
        fetch(task)
        spec = DTaskSpec(Argument[Argument(1, identity), Argument(2, In(raw_big)), Argument(3, In(raw_small))],
                          Options())

        function locality_run(bias; ntasks=8)
            old_bias = Dagger.DATADEPS_LOCALITY_BIAS[]
            Dagger.DATADEPS_LOCALITY_BIAS[] = bias
            try
                sched = RoundRobinScheduler()
                return map(1:ntasks) do _
                    proc = datadeps_schedule_task(sched, state, all_procs, all_scope, task_scope, spec, task)
                    only(Dagger.memory_spaces(proc))
                end
            finally
                Dagger.DATADEPS_LOCALITY_BIAS[] = old_bias
            end
        end

        seq0 = locality_run(0.0)
        seq_half = locality_run(0.5)
        seq1 = locality_run(1.0)

        # `bias=0.0` must be a *genuine* no-op: byte-for-byte the same choices
        # plain round robin (no locality machinery at all) would make.
        @test seq0 == [CPURAMMemorySpace(mod1(i, 4)) for i in 1:8]

        # `bias=1.0` is pure locality: always the fully-resident space,
        # regardless of rotation position.
        @test all(==(CPURAMMemorySpace(4)), seq1)

        # `bias=0.5` actually blends -- neither ignoring locality (like 0.0)
        # nor ignoring rotation distance (like 1.0). This is the assertion
        # that makes the knob real rather than merely documented: an earlier
        # version scored `bias * weight` with no competing term, under which
        # `argmax` is invariant to a positive scalar and every `bias > 0`
        # (including 0.5) behaved exactly like `bias = 1.0`.
        @test seq_half != seq0
        @test seq_half != seq1
        @test length(Set([seq0, seq_half, seq1])) == 3
    end
end
