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

@testset "In-place receives validate their MPI datatype count" begin
    # Exercise empty messages and multi-byte/derived element types through the
    # production receive path, rather than only checking MPI.Get_count itself.
    for source in (UInt8[], UInt8[1, 2, 3], [1.25, -2.5, 7.0],
                   ComplexF64[1+2im, 3-4im], [(1.0, 2.0), (3.0, 4.0)])
        dest = similar(source)
        request = MPI.Isend(source, MPI.COMM_SELF; dest=0, tag=74)
        result, inplace = MPIExt.recv_yield!(dest, MPI.COMM_SELF, 0, UInt32(74))
        MPI.Wait(request)
        @test inplace
        @test result === dest
        @test dest == source
    end
end
