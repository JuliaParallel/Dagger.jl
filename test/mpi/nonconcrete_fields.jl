# A struct field declared with a non-concrete type (here, untyped) may or may
# not hold mutable memory, so Datadeps decides from each value. A value that
# holds none must be passed through untracked: its aliasing has no address, so
# tracking it would give every such value the same key in the aliased-object
# cache, and a task would receive another task's value.
struct UntypedFieldVal; part::Int; x; end
untyped_field_val(part::Int) = UntypedFieldVal(part, part)
function check_untyped_field_val!(buf::Vector{Float64}, x::UntypedFieldVal)
    buf[1] = x.part
    buf[2] = x.x
    return
end

@testset "Values of non-concrete fields keep their own value" begin
    owner_of(i) = (i - 1) ÷ 2
    P = 2 * nranks
    bufs = [Dagger.tochunk(zeros(2), proc_for_rank(owner_of(i)), space_for_rank(owner_of(i))) for i in 1:P]
    # From upstream tasks (as `Chunk`s), and as plain values
    for vals in ([Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner_of(i)), untyped_field_val(i)) for i in 1:P],
                 [untyped_field_val(i) for i in 1:P])
        Dagger.spawn_datadeps() do
            for i in 1:P
                Dagger.@spawn scope=Dagger.scope(mpi_rank=owner_of(i)) check_untyped_field_val!(InOut(bufs[i]), vals[i])
            end
        end
        seen = [fetch(Dagger.@spawn(scope=Dagger.scope(mpi_rank=owner_of(i)), copy(bufs[i]))) for i in 1:P]
        @test seen == [[i, i] for i in 1:P]
    end
end
