include(joinpath(@__DIR__, "stencil_defs.jl"))

@testset "CPU" begin
    test_stencil()
end

# N.B. CPU only: GPU sparse tiles have no device-side `setindex!`, so a kernel
# cannot sweep them. See the comment on `test_stencil_sparse`.
@testset "CPU (sparse)" begin
    test_stencil_sparse()
end

@testset "GPU" begin
    for (kind, scope) in GPU_SCOPES
        # FIXME
        kind == :oneAPI && continue
        @testset "$kind" begin
            Dagger.with_options(;scope) do
                # The Metal backend breaks on the 3D/4D stencil tests and
                # causes subsequent tests to fail, so skip them there.
                test_stencil(; skip_highdim=(kind == :Metal || kind == :ROCm))
                @testset "sparse" begin
                    test_stencil_sparse_gpu()
                end
            end
        end
    end
end
