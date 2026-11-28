# Under uniform planning, the aliasing info of the remote slots a window of tasks needs is
# exchanged in one batch (schedule_slot_window!). Tasks are spread round-robin over all ranks,
# so most of their arguments need a slot on another rank. The result must be what sequential
# execution gives, across several windows and with arguments owned by different ranks.
window_add!(x, y, k) = (x .+= k .* y; nothing)

@testset "Windowed slot aliasing exchange" begin
    ntasks = 3 * Dagger.DATADEPS_SLOT_WINDOW + 17
    narr = 2 * nranks
    @testset "hierarchical=$hierarchical" for hierarchical in (false, true)
        arrs = [Dagger.tochunk(fill(float(j), 4), proc_for_rank(mod(j, nranks)),
                               space_for_rank(mod(j, nranks))) for j in 1:narr]
        # Every task writes one array and reads its neighbour, owned by another rank, so one
        # of the two is a remote slot for whichever rank runs the task
        Dagger.spawn_datadeps(; hierarchical) do
            for i in 1:ntasks
                Dagger.spawn(window_add!, InOut(arrs[mod1(i, narr)]),
                             In(arrs[mod1(i + 1, narr)]), i)
            end
        end
        vals = [fill(float(j), 4) for j in 1:narr]
        for i in 1:ntasks
            vals[mod1(i, narr)] .+= i .* vals[mod1(i + 1, narr)]
        end
        for j in 1:narr
            @test fetch(arrs[j]) == vals[j]
        end
    end
end

# The window is really used: the aliasing infos of remote slots arrive through `schedule_slot_window!`
# (counted by the `:slot_window_ainfos` operation counter), and a scheduler that is not stateless never
# takes that path.
@testset "Slot window is taken, and only for stateless schedulers" begin
    push!(Dagger.OPCOUNTER_CATEGORIES, :slot_window_ainfos)
    try
        count() = Dagger.OPCOUNTER_slot_window_ainfos.value[]
        narr = 2 * nranks
        run(; scheduler=nothing) = begin
            arrs = [Dagger.tochunk(fill(float(j), 4), proc_for_rank(mod(j, nranks)),
                                   space_for_rank(mod(j, nranks))) for j in 1:narr]
            c0 = count()
            Dagger.spawn_datadeps(; scheduler) do
                for i in 1:(2 * Dagger.DATADEPS_SLOT_WINDOW)
                    Dagger.spawn(window_add!, InOut(arrs[mod1(i, narr)]), In(arrs[mod1(i + 1, narr)]), i)
                end
            end
            foreach(fetch, arrs)
            return count() - c0
        end
        if nranks > 1
            @test run() > 0
        end
        # a scheduler that reads the planning state is never asked ahead of time
        struct_free = Dagger.datadeps_schedule_is_stateless(Dagger.RoundRobinScheduler())
        @test struct_free
        @test !Dagger.datadeps_schedule_is_stateless(Dagger.NaiveScheduler())
    finally
        filter!(!=(:slot_window_ainfos), Dagger.OPCOUNTER_CATEGORIES)
    end
end
