include("deadline.jl")
# A scope that names this rank means the same in the shared and in a rank-local graph
within(120, "rank scopes inside a rank-local graph") do
@testset "rank scopes inside a rank-local graph" begin
    me = MPI.Comm_rank(comm); other = (me + 1) % MPI.Comm_size(comm)
    Dagger.with_rank_local() do
        # the rank's own world scope runs on this graph's processors
        @test fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=me), 1 + 1)) == 2
        # a world processor of this rank is the same thread of the local graph
        proc = proc_for_rank(me)
        @test fetch(Dagger.@spawn(scope=Dagger.ExactScope(proc), Threads.threadid())) == proc.innerProc.tid
        # another rank's scope still fails loudly
        nranks > 1 && (msg = try fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=other), 1 + 1)); "no error" catch e; sprint(showerror, e) end; @test msg != "no error"; @test occursin("No processors available", msg))
    end
end
end
