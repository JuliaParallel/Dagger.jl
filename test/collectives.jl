# Without MPI the collectives reduce to their one-process meaning (the MPI behaviour is tested in test/mpi/collectives.jl).
@testset "Collectives without MPI" begin
    @test Dagger.barrier() === nothing
    @test Dagger.barrier(; comm=nothing) === nothing
    @test Dagger.bcast(3) == 3
    @test Dagger.bcast("x"; root=0) == "x"
    @test_throws ArgumentError Dagger.bcast(3; root=1)
    @test Dagger.allgather(3) == Any[3]
    @test Dagger.allgather(Int[]) == Any[Int[]]
    @test Dagger.gather(3) == Any[3]
    @test Dagger.gather(3; root=0) == Any[3]
    @test_throws ArgumentError Dagger.gather(3; root=1)
    @test Dagger.scatter([7]) == 7
    @test Dagger.scatter([7]; root=0) == 7
    @test_throws ArgumentError Dagger.scatter([7, 8])
    @test_throws ArgumentError Dagger.scatter([7]; root=1)
    @test Dagger.alltoall(["a"]) == Any["a"]
    @test_throws ArgumentError Dagger.alltoall([1, 2])
    @test Dagger.allreduce(+, 3) == 3
    @test Dagger.allreduce(+, 3; init=4) == 7
    @test Dagger.allreduce(*, "a"; init="b") == "ba"
    @test Dagger.reduce_to(+, 3) == 3
    @test Dagger.reduce_to(+, 3; root=0, init=4) == 7
    @test_throws ArgumentError Dagger.reduce_to(+, 3; root=2)
    # the task form reduces to the local list
    @test Dagger.allgather_tasks(Dagger.DTask[]) == []
    let ts = [Dagger.@spawn(identity(i)) for i in 1:4]
        @test Dagger.allgather_tasks(ts) == [1, 2, 3, 4]
        got = Dagger.allgather(ts, Int)
        @test got isa Vector{Int} && got == [1, 2, 3, 4]
        @test Dagger.allgather(ts, Float64) == [1.0, 2.0, 3.0, 4.0]
    end
    @test Dagger.allgather([1, 2]) == Any[[1, 2]]     # a vector alone is a value, not a task list
    let ts = [Dagger.@spawn(error("boom")), Dagger.@spawn(identity(1))]
        @test_throws Exception Dagger.allgather_tasks(ts)
    end
    # from inside a task
    @test fetch(Dagger.@spawn(Dagger.allgather(5))) == Any[5]
end
