@testset "With/Without Transpose" begin
    X = rand(40, 40)
    tol = 1e-12
    X1 = distribute(X, Blocks(10, 20))
    X2 = X1'*X1
    X3 = X1*X1'
    X4 = X1*X1
    @test norm(collect(X2) - (X' * X)) < tol
    @test norm(collect(X3) - (X * X')) < tol
    @test norm(collect(X4) - (X * X)) < tol
end

@testset "Powers" begin
    x = rand(Blocks(4,4), 16, 16)
    @test collect(x^1) == collect(x)
    @test collect(x^2) == collect(x*x)
    @test collect(x^3) == collect(x*x*x)
end

function test_gemm!(AT, T, szA, szB, partA, partB)
    @assert szA[1] == szB[2]
    szC = (szA[1], szA[1])
    @assert partA.blocksize[1] == partB.blocksize[2]
    partC = Blocks(partA.blocksize[1], partB.blocksize[2])

    A = AT(rand(T, szA...))
    B = AT(rand(T, szB...))

    DA = distribute(A, partA)
    DB = distribute(B, partB)

    ## Out-of-place gemm
    # No transA, No transB
    DC = DA * DB
    C = A * B
    @test collect(DC) ≈ C

    if szA == szB
        # No transA, transB
        DC = DA * DB'
        C = A * B'
        @test collect(DC) ≈ C

        # transA, No transB
        DC = DA' * DB
        C = A' * B
        @test collect(DC) ≈ C
    end

    # transA, transB
    DC = DA' * DB'
    C = A' * B'
    @test collect(DC) ≈ C

    ## In-place gemm
    # No transA, No transB
    C = AT(zeros(T, szC...))
    DC = distribute(C, partC)
    mul!(C, A, B)
    mul!(DC, DA, DB)
    @test collect(DC) ≈ C

    if szA == szB
        # No transA, transB
        C = AT(zeros(T, szC...))
        DC = distribute(C, partC)
        mul!(C, A, B')
        mul!(DC, DA, DB')
        @test collect(DC) ≈ C

        # transA, No transB
        C = AT(zeros(T, szC...))
        DC = distribute(C, partC)
        mul!(C, A', B)
        mul!(DC, DA', DB)
        @test collect(DC) ≈ C
    end

    # transA, transB
    C = AT(zeros(T, szA[2], szA[2]))
    DC = distribute(C, partC)
    mul!(C, A', B')
    mul!(DC, DA', DB')
    collect(DC) ≈ C

    if szA == szB
        ## Out-of-place syrk
        # No trans, trans
        DC = DA * DA'
        C = A * A'
        @test collect(DC) ≈ C

        # trans, No trans
        DC = DA' * DA
        C = A' * A
        @test collect(DC) ≈ C

        ## In-place syrk
        # No trans, trans
        C = AT(zeros(T, szC...))
        DC = distribute(C, partC)
        mul!(C, A, A')
        mul!(DC, DA, DA')
        @test collect(DC) ≈ C

        # trans, No trans
        C = AT(zeros(T, szC...))
        DC = distribute(C, partC)
        mul!(C, A', A)
        mul!(DC, DA', DA)
        @test collect(DC) ≈ C
    end
end

_sizes_to_test = [
    (4, 4),
    (7, 7),
    (12, 12),
    (16, 16),
]
size_sets_to_test = map(_sizes_to_test) do sz
    rows, cols = sz
    return [
        (rows, cols) => (cols, rows),
        (rows ÷ 2, cols) => (cols, rows ÷ 2),
        (rows, cols ÷ 2) => (cols ÷ 2, rows),
    ]
end
sizes_to_test = vcat(size_sets_to_test...)
part_sets_to_test = map(_sizes_to_test) do sz
    rows, cols = sz
    return [
        Blocks(rows, cols) => Blocks(cols, rows),
        Blocks(rows ÷ 2, cols) => Blocks(cols, rows ÷ 2),
        Blocks(rows, cols ÷ 2) => Blocks(cols ÷ 2, rows),
    ]
end
parts_to_test = vcat(part_sets_to_test...)

for (kind, AT, scope) in ALL_SCOPES
    kind == :oneAPI || kind == :Metal || kind == :OpenCL && continue
    @testset "$kind" begin
        Dagger.with_options(;scope) do
            @testset "Size=$szA*$szB" for (szA, szB) in sizes_to_test
                @testset "Partitioning=$partA*$partB" for (partA,partB) in parts_to_test
                    @testset "T=$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
                        test_gemm!(AT, T, szA, szB, partA, partB)
                    end
                end
            end
        end
    end
end