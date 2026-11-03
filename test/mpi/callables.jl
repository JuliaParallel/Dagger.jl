# A callable's planning representation must survive moves on spectator ranks.
@testset "MPI replicated callables" begin
    let comm=MPI.COMM_WORLD
        for f in (identity, Float64, Dagger.Kernel(identity)),
            src_rank in 0:MPI.Comm_size(comm)-1, dst_rank in 0:MPI.Comm_size(comm)-1
            dst=MPIExt.MPIProcessor(Dagger.ThreadProc(1,1),comm,dst_rank)
            for src in (MPIExt.MPIOSProc(comm,src_rank),
                        MPIExt.MPIProcessor(Dagger.ThreadProc(1,1),comm,src_rank))
                @test Dagger.move(src,dst,f) === f
            end
        end
    end
end
