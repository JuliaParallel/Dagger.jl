function test_linalg_core(AT)
    @testset "isapprox" begin
        A = AT(rand(16, 16))

        U1 = UpperTriangular(DArray(A, Blocks(16, 16)))
        U2 = UpperTriangular(DArray(A, Blocks(16, 16)))
        @test isapprox(U1, U2)

        L1 = LowerTriangular(DArray(A, Blocks(16, 16)))
        L2 = LowerTriangular(DArray(A, Blocks(16, 16)))
        @test isapprox(L1, L2)
    end
end

for (kind, AT, scope) in ALL_SCOPES
    kind == :oneAPI || kind == :Metal || kind == :OpenCL && continue
    @testset "$kind" begin
        Dagger.with_options(;scope) do
            test_linalg_core(AT)
        end
    end
end