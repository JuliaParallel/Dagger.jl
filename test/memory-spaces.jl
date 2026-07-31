@testset "Memory Spaces" begin
    @testset "Object Queries" begin
        # OSProc
        x = 123
        @test Dagger.memory_space(x) == Dagger.CPURAMMemorySpace(1)
        if nprocs() > 1
            @test remotecall_fetch(Dagger.memory_space, 2, x) == Dagger.CPURAMMemorySpace(2)
        end

        # ThreadProc
        x = Dagger.tochunk(123)
        @test Dagger.memory_space(x) == Dagger.CPURAMMemorySpace(1)
        if nprocs() > 1
            @test remotecall_fetch(Dagger.memory_space, 2, x) == Dagger.CPURAMMemorySpace(1)
        end

        if nprocs() > 1
            x = remotecall_fetch(Dagger.tochunk, 2, 123)
            @test Dagger.memory_space(x) == Dagger.CPURAMMemorySpace(2)
            @test remotecall_fetch(Dagger.memory_space, 2, x) == Dagger.CPURAMMemorySpace(2)
        end

        x = Dagger.@spawn scope=Dagger.scope(worker=1) identity(123)
        @test Dagger.memory_space(x) == Dagger.CPURAMMemorySpace(1)
        if nprocs() > 1
            @test remotecall_fetch(Dagger.memory_space, 2, x) == Dagger.CPURAMMemorySpace(1)
        end

        if nprocs() > 1
            x = Dagger.@spawn scope=Dagger.scope(worker=2) identity(123)
            @test Dagger.memory_space(x) == Dagger.CPURAMMemorySpace(2)
            @test remotecall_fetch(Dagger.memory_space, 2, x) == Dagger.CPURAMMemorySpace(2)
        end
    end
    @testset "Processor Queries" begin
        w1_t1_proc = Dagger.ThreadProc(1,1)
        w1_t2_proc = Dagger.ThreadProc(1,2)
        if nprocs() > 1
            w2_t1_proc = Dagger.ThreadProc(2,1)
            w2_t2_proc = Dagger.ThreadProc(2,2)
        end
        @test Dagger.memory_spaces(w1_t1_proc) == Set([Dagger.CPURAMMemorySpace(1)])
        @test Dagger.memory_spaces(w1_t2_proc) == Set([Dagger.CPURAMMemorySpace(1)])
        if nprocs() > 1
            @test Dagger.memory_spaces(w2_t1_proc) == Set([Dagger.CPURAMMemorySpace(2)])
            @test Dagger.memory_spaces(w2_t2_proc) == Set([Dagger.CPURAMMemorySpace(2)])
        end
        @test only(Dagger.memory_spaces(w1_t1_proc)) == only(Dagger.memory_spaces(w1_t2_proc))
        if nprocs() > 1
            @test only(Dagger.memory_spaces(w2_t1_proc)) != only(Dagger.memory_spaces(w1_t1_proc))
        end
        @test_throws ArgumentError Dagger.memory_spaces(FakeProc())

        w1_mem = Dagger.CPURAMMemorySpace(1)
        @test Set(Dagger.processors(w1_mem)) == filter(proc->proc isa Dagger.ThreadProc, Dagger.get_processors(OSProc(1)))
        if nprocs() > 1
            w2_mem = Dagger.CPURAMMemorySpace(2)
            @test Set(Dagger.processors(w2_mem)) == filter(proc->proc isa Dagger.ThreadProc, Dagger.get_processors(OSProc(2)))
        end
    end
end
