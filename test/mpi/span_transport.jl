function transport_selection_spans(array, indices, triangular)
    triangular || return Dagger.memory_spans(view(array, indices...))
    first_row, first_column = first.(indices)
    spans = Dagger.memory_spans(view(array, first_row:first_row, first_column:first_column))
    for column in 2:length(indices[2])
        append!(spans, Dagger.memory_spans(view(array, first_row:first_row+column-1,
                                              first_column+column-1:first_column+column-1)))
    end
    return spans
end

# Shared by MPI GPU suites and standalone device-transport validation.
function test_mpi_span_transport(to_device, to_host, ::Type{FT}; comm=MPI.COMM_WORLD) where FT
    rank = MPI.Comm_rank(comm)
    MPI.Comm_size(comm) >= 2 || return nothing
    mpi_extension = Base.get_extension(Dagger, :MPIExt)
    @testset "Partial MPI transport preserves untouched elements" begin
        for T in (FT, Complex{FT}, UInt8), (source_gpu, destination_gpu) in
            ((true, true), (true, false), (false, true)), selection in 1:3
            original = reshape(T.(mod.(1:768, 127) .+ 1), 24, 32)
            initial = fill(T(13), size(original))
            source = source_gpu ? to_device(original) : copy(original)
            destination = destination_gpu ? to_device(initial) : copy(initial)
            expected = copy(initial)
            source_indices = selection == 1 ? (2:16, 3:21) : selection == 2 ? (2:17, 3:21) : (2:13, 2:13)
            destination_indices = selection == 1 ? (3:17, 4:22) : selection == 2 ? (3:18, 4:22) : (3:14, 4:15)
            if selection == 3
                for j in 1:12, i in 1:j
                    expected[i + 2, j + 3] = original[i + 1, j + 1]
                end
            else
                copyto!(view(expected, destination_indices...), view(original, source_indices...))
            end
            if rank == 0
                spans = Dagger._with_default_acceleration() do
                    transport_selection_spans(source, source_indices, selection == 3)
                end
                mpi_extension.send_spans(source, spans, Dagger.value_memory_space(source), comm, 1, 1937)
                @test (source_gpu ? to_host(source) : source) == original
            elseif rank == 1
                spans = Dagger._with_default_acceleration() do
                    transport_selection_spans(destination, destination_indices, selection == 3)
                end
                mpi_extension.recv_spans!(destination, spans, Dagger.value_memory_space(destination), comm, 0, 1937)
                @test (destination_gpu ? to_host(destination) : destination) == expected
            end
            # Spectator ranks must also progress Dagger's MPI relay while waiting.
            request = MPI.Ibarrier(comm)
            while !MPI.Test(request)
                sleep(0.001)
            end
        end
    end
    return nothing
end
