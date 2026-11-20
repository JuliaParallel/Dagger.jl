# Datadeps arguments whose aliasing is `NoAliasing` share one aliasing identity; each must still reach its own task
# (app entry 45: with several in flight, writer i received the value of another partition).
struct NoAliasVal; part::Int; v::Vector{Float64}; end
Dagger.aliasing(::NoAliasVal) = Dagger.NoAliasing()
mk_noalias(part::Int) = NoAliasVal(part, fill(Float64(part), 10 + part))
check_noalias!(buf::Vector{Float64}, x::NoAliasVal, expect::Int) = (buf[1] = x.part == expect ? 0.0 : 1.0; buf[2] = x.part; nothing)

@testset "NoAliasing arguments of one Datadeps region keep their own value" begin
    nr = MPI.Comm_size(comm)
    P = 2 * nr
    owner_of(i) = (i - 1) ÷ 2
    procs_by_rank = Dict(r => first(filter(p -> p.rank == r, mpi_procs())) for r in 0:nr-1)
    bufs = [Dagger.tochunk(zeros(2), procs_by_rank[owner_of(i)], Dagger.memory_space(procs_by_rank[owner_of(i)])) for i in 1:P]
    vals = [Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner_of(i)), mk_noalias(i)) for i in 1:P]
    Dagger.spawn_datadeps() do
        for i in 1:P
            Dagger.@spawn scope=Dagger.scope(mpi_rank=owner_of(i)) check_noalias!(Dagger.InOut(bufs[i]), vals[i], i)
        end
    end
    seen = [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner_of(i)), copy(bufs[i]))) for i in 1:P]
    @test all(s -> s[1] == 0.0, seen)
    @test [Int(s[2]) for s in seen] == collect(1:P)
end
