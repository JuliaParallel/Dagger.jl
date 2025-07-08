using FFTW

function test_fft()
    @testset "FFT" begin
        @testset for T in (ComplexF64, ComplexF32)
            @testset "1D" begin
                # Out-of-place
                A = rand(T, 100)
                DA = DArray(A)
                B = fft(A)
                DB = fft(DA)
                @test DB isa DVector{T}
                @test B ≈ collect(DB)

                # In-place
                A = rand(T, 100)
                DA = DArray(A)
                fft!(A)
                fft!(DA)
                @test A ≈ collect(DA)
            end

            @testset "2D" begin
                # Out-of-place
                A = rand(T, 100, 100)
                DA = DArray(A)
                B = fft(A)
                DB = fft(DA)
                @test DB isa DMatrix{T}
                @test B ≈ collect(DB)

                # In-place
                A = rand(T, 100, 100)
                DA = DArray(A)
                fft!(A)
                fft!(DA)
                @test A ≈ collect(DA)
            end

            @testset "3D" begin
                # Out-of-place (Pencil)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                B = fft(A)
                DB = fft(DA; decomp=:pencil)
                @test DB isa DArray{T, 3}
                @test B ≈ collect(DB)

                # Out-of-place (Slab)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                B = fft(A)
                DB = fft(DA; decomp=:slab)
                @test DB isa DArray{T, 3}
                @test B ≈ collect(DB)

                # In-place (Pencil)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                fft!(A)
                fft!(DA; decomp=:pencil)
                @test A ≈ collect(DA)

                # In-place (Slab)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                fft!(A)
                fft!(DA; decomp=:slab)
                @test A ≈ collect(DA)
            end
        end
    end

    @testset "IFFT" begin
        for T in (ComplexF64, ComplexF32)
            @testset "1D" begin
                # Out-of-place
                A = rand(T, 100)
                DA = DArray(A)
                B = ifft(A)
                DB = ifft(DA)
                @test DB isa DVector{T}
                @test B ≈ collect(DB)

                # In-place
                A = rand(T, 100)
                DA = DArray(A)
                ifft!(A)
                ifft!(DA)
                @test A ≈ collect(DA)
            end

            @testset "2D" begin
                # Out-of-place
                A = rand(T, 100, 100)
                DA = DArray(A)
                B = ifft(A)
                DB = ifft(DA)
                @test DB isa DMatrix{T}
                @test B ≈ collect(DB)

                # In-place
                A = rand(T, 100, 100)
                DA = DArray(A)
                ifft!(A)
                ifft!(DA)
                @test A ≈ collect(DA)
            end

            @testset "3D" begin
                # Out-of-place (Pencil)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                B = ifft(A)
                DB = ifft(DA; decomp=:pencil)
                @test DB isa DArray{T, 3}
                @test B ≈ collect(DB)

                # Out-of-place (Slab)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                B = ifft(A)
                DB = ifft(DA; decomp=:slab)
                @test DB isa DArray{T, 3}
                @test B ≈ collect(DB)

                # In-place (Pencil)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                ifft!(A)
                ifft!(DA; decomp=:pencil)
                @test A ≈ collect(DA)

                # In-place (Slab)
                A = rand(T, 100, 100, 100)
                DA = DArray(A)
                ifft!(A)
                ifft!(DA; decomp=:slab)
                @test A ≈ collect(DA)
            end
        end
    end
end

@testset "CPU" begin
    test_fft()
end

for (kind, scope) in GPU_SCOPES
    kind == :CUDA || kind == :ROCm || continue
    @testset "$kind" begin
        Dagger.with_options(;scope) do
            test_fft()
        end
    end
end
