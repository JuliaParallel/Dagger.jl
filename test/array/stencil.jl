include(joinpath(@__DIR__, "stencil_defs.jl"))

@testset "CPU" begin
    test_stencil()
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
            end
        end
    end
end
