# Fail source conversion before MPI posts a receive, after the stream guard
# has been acquired. This exercises error cleanup without corrupting the wire.
struct InvalidMPIReceiveSource <: Integer end
struct MPIReceiveConversionError <: Exception end
Base.Int(::InvalidMPIReceiveSource) = 0
Base.cconvert(::Type{Cint}, ::InvalidMPIReceiveSource) = throw(MPIReceiveConversionError())

@testset "MPI receive stream ownership" begin
    key = (MPI.COMM_SELF, 0, 1)

    @testset "Uncontended receives need no event" begin
        @test (@inferred MPIExt.acquire_recv!(key)) === nothing
        @lock MPIExt.RECV_WAITING begin
            @test haskey(Dagger.payload(MPIExt.RECV_WAITING), key)
            @test Dagger.payload(MPIExt.RECV_WAITING)[key] === nothing
        end
        @test (@inferred MPIExt.release_recv!(key)) === nothing
        @lock MPIExt.RECV_WAITING begin
            @test !haskey(Dagger.payload(MPIExt.RECV_WAITING), key)
        end
    end

    @testset "Contenders serialize without losing wakeups" begin
        MPIExt.acquire_recv!(key)
        active = Threads.Atomic{Int}(0)
        tasks = [Threads.@spawn begin
            MPIExt.acquire_recv!(key)
            sole_owner = Threads.atomic_add!(active, 1) == 0
            try
                sleep(0.001)
                sole_owner
            finally
                Threads.atomic_sub!(active, 1)
                MPIExt.release_recv!(key)
            end
        end for _ in 1:8]
        status = timedwait(10.0; pollint=0.001) do
            @lock MPIExt.RECV_WAITING begin
                Dagger.payload(MPIExt.RECV_WAITING)[key] isa Base.Event
            end
        end
        @test status === :ok
        MPIExt.release_recv!(key)
        @test timedwait(() -> all(istaskdone, tasks), 10.0; pollint=0.001) === :ok
        @test all(fetch, tasks)
        @test active[] == 0
        @lock MPIExt.RECV_WAITING begin
            @test !haskey(Dagger.payload(MPIExt.RECV_WAITING), key)
        end
    end

    @testset "A failed receive releases its stream" begin
        source = InvalidMPIReceiveSource()
        @test_throws MPIReceiveConversionError MPIExt.recv_yield(MPI.COMM_SELF, source, UInt32(1))
        @lock MPIExt.RECV_WAITING begin
            @test !haskey(Dagger.payload(MPIExt.RECV_WAITING), key)
        end
        @test_throws MPIReceiveConversionError MPIExt.recv_yield!(zeros(1), MPI.COMM_SELF, source, UInt32(1))
        @lock MPIExt.RECV_WAITING begin
            @test !haskey(Dagger.payload(MPIExt.RECV_WAITING), key)
        end
    end
end
