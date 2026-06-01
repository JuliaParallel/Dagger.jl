using MPI, Dagger, Test

Dagger.accelerate!(:mpi)

comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)
sz = MPI.Comm_size(comm)

@testset "MPI Dagger P2P" begin
    if sz == 2
        tag = 42
        if rank == 0
            A = rand(2, 2)
            Dagger.send_yield(A, comm, 1, tag)
        elseif rank == 1
            B = Dagger.recv_yield(comm, 0, tag)
            @test size(B) == (2, 2)
        end
    end
end

MPI.Finalize()
