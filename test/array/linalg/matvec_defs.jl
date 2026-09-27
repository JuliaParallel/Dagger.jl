# Shared CPU/GPU regression: dense GPU tiles must never reach host BLAS.
function test_dense_matvec(; gpu=false)
    @testset "Dense matvec T=$T blocks=$bs" for T in (Float32, ComplexF32), bs in (2, 4)
        A = T[1 2 3 4; 5 6 7 8; 9 10 11 12; 13 14 15 16]
        if T <: Complex
            A .+= im .* transpose(real.(A))
        end
        x = T[1, -2, 3, -4]
        initial = T[4, 3, 2, 1]
        DA = Dagger.distribute(A, Dagger.Blocks(bs, bs))
        Dx = Dagger.distribute(x, Dagger.Blocks(bs))
        Dy = Dagger.distribute(initial, Dagger.Blocks(bs))
        wait(DA); wait(Dx); wait(Dy)
        if gpu
            for arr in (DA, Dx, Dy), chunk in arr.chunks
                @test !(Dagger.chunktype(fetch(chunk; raw=true)) <: Array)
            end
        end
        for op in (identity, transpose, adjoint)
            result = op(DA) * Dx
            @test collect(result) ≈ op(A) * x
            if gpu
                for chunk in result.chunks
                    @test !(Dagger.chunktype(fetch(chunk; raw=true)) <: Array)
                end
            end
            copyto!(Dy, Dagger.distribute(initial, Dagger.Blocks(bs)))
            @test mul!(Dy, op(DA), Dx, T(2), T(3)) === Dy
            @test collect(Dy) ≈ 2 .* (op(A) * x) .+ 3 .* initial
        end
    end
end
