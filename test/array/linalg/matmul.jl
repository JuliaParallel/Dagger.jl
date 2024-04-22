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

function test_gemm!(T, szA, szB, partA, partB)
    @assert szA[1] == szB[2]
    szC = (szA[1], szA[1])
    @assert partA.blocksize[1] == partB.blocksize[2]
    partC = Blocks(partA.blocksize[1], partB.blocksize[2])

    A = rand(T, szA...)
    B = rand(T, szB...)

    DA = distribute(A, partA)
    DB = distribute(B, partB)

    SA = sprand(T, szA..., 0.1)
    SB = sprand(T, szA..., 0.1)

    DSA = distribute(SA, partA)
    DSB = distribute(SB, partB)

    ## Out-of-place gemm
    # No transA, No transB
    # Dense
    DC = DA * DB
    C = A * B
    @test collect(DC) ≈ C
    # Sparse
    DSC = DSA * DSB
    SC = SA * SB
    @test collect(DSC) ≈ SC

    if szA == szB
        # No transA, transB
        # Dense
        DC = DA * DB'
        C = A * B'
        @test collect(DC) ≈ C
        # Sparse
        DSC = DSA * DSB'
        SC = SA * SB'
        @test collect(DSC) ≈ SC

        # transA, No transB
        # Dense
        DC = DA' * DB
        C = A' * B
        @test collect(DC) ≈ C
        # Sparse
        DSC = DSA' * DSB
        SC = SA' * SB
        @test collect(DSC) ≈ SC
    end

    # transA, transB
    # Dense
    DC = DA' * DB'
    C = A' * B'
    @test collect(DC) ≈ C
    #= Sparse
    DSC = DSA' * DSB'
    SC = SA' * SB'
    @test collect(DSC) ≈ SC
    =#

    ## In-place gemm
    # No transA, No transB
    # Dense
    C = zeros(T, szC...)
    DC = distribute(C, partC)
    mul!(C, A, B)
    mul!(DC, DA, DB)
    @test collect(DC) ≈ C
    #= Sparse
    SC = zeros(T, szC...)
    DSC = distribute(SC, partC)
    mul!(SC, SA, SB)
    mul!(DSC, DSA, DSB)
    @test collect(DSC) ≈ SC
    =#

    if szA == szB
        # No transA, transB
        C = zeros(T, szC...)
        DC = distribute(C, partC)
        mul!(C, A, B')
        mul!(DC, DA, DB')
        @test collect(DC) ≈ C

        # transA, No transB
        C = zeros(T, szC...)
        DC = distribute(C, partC)
        mul!(C, A', B)
        mul!(DC, DA', DB)
        @test collect(DC) ≈ C
    end

    # transA, transB
    C = zeros(T, szA[2], szA[2])
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
        C = zeros(T, szC...)
        DC = distribute(C, partC)
        mul!(C, A, A')
        mul!(DC, DA, DA')
        @test collect(DC) ≈ C

        # trans, No trans
        C = zeros(T, szC...)
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
@testset "GEMM" begin
    @testset "Size=$szA*$szB" for (szA, szB) in sizes_to_test
        @testset "Partitioning=$partA*$partB" for (partA,partB) in parts_to_test
            @testset "T=$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
                test_gemm!(T, szA, szB, partA, partB)
            end
        end
    end
end

function test_gemv!(T, szA, szB, partA, partB)
    @assert szA[2] == szB[1]
    szC = (szA[1],)
    @assert partA.blocksize[2] == partB.blocksize[1]
    partC = Blocks(partA.blocksize[1],)

    A = rand(T, szA...)
    B = rand(T, szB...)

    DA = distribute(A, partA)
    DB = distribute(B, partB)

    ## Out-of-place gemm
    # No transA
    DC = DA * DB
    C = A * B
    @test collect(DC) ≈ C

    if szA[1] == szB[1]
        # transA
        DC = DA' * DB
        C = A' * B
        @test collect(DC) ≈ C
    end

    ## In-place gemm
    # No transA
    C = zeros(T, szC...)
    DC = distribute(C, partC)
    mul!(C, A, B)
    mul!(DC, DA, DB)
    @test collect(DC) ≈ C

    if szA[1] == szB[1]
        # transA
        C = zeros(T, szC...)
        DC = distribute(C, partC)
        mul!(C, A', B)
        mul!(DC, DA', DB)
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
        (rows, cols) => (cols,),
        (rows, cols ÷ 2) => (cols ÷ 2,),
    ]
end
sizes_to_test = vcat(size_sets_to_test...)
part_sets_to_test = map(_sizes_to_test) do sz
    rows, cols = sz
    return [
        Blocks(rows, cols) => Blocks(cols,),
        Blocks(rows, cols ÷ 2) => Blocks(cols ÷ 2,),
    ]
end
parts_to_test = vcat(part_sets_to_test...)
@testset "GEMV" begin
    @testset "Size=$szA*$szB" for (szA, szB) in sizes_to_test
        @testset "Partitioning=$partA*$partB" for (partA,partB) in parts_to_test
            @testset "T=$T" for T in (Float32, Float64, ComplexF32, ComplexF64)
                test_gemv!(T, szA, szB, partA, partB)
            end
        end
    end
end
