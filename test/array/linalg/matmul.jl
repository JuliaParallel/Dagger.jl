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
    SB = sprand(T, szB..., 0.1)

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
    (12, 12),
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
            @testset "T=$T" for T in (Float64, ComplexF64)
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
        # transA (square / matching inner dim for A')
        DC = DA' * DB
        C = A' * B
        @test collect(DC) ≈ C
    end

    # Tall/wide A': b matches A's row count, result matches A's column count
    if szA[1] != szA[2]
        B2 = rand(T, szA[1])
        DB2 = distribute(B2, Blocks(partA.blocksize[1]))
        DC2 = DA' * DB2
        C2 = A' * B2
        @test collect(DC2) ≈ C2
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
    (12, 12),
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
            @testset "T=$T" for T in (Float64, ComplexF64)
                test_gemv!(T, szA, szB, partA, partB)
            end
        end
    end
end

# Mixed eltypes: LinearAlgebra's `mul!` promotes (PETSc-style FP32 operator ×
# FP64 vectors) without the caller converting every operand first. Same-type
# GEMM above stays on the `Matrix{T}` BLAS path.
@testset "Mixed-precision mul!" begin
    n = 12
    partA = Blocks(4, 4)
    partV = Blocks(4)

    A32 = rand(Float32, n, n)
    x64 = rand(Float64, n)
    DA32 = distribute(A32, partA)
    Dx64 = distribute(x64, partV)

    y = DA32 * Dx64
    @test y isa Dagger.DVector{Float64}
    @test collect(y) ≈ A32 * x64

    Dy = distribute(zeros(Float64, n), partV)
    mul!(Dy, DA32, Dx64)
    @test collect(Dy) ≈ A32 * x64

    mul!(Dy, DA32, Dx64, 2.0, 0.0)
    @test collect(Dy) ≈ 2 .* (A32 * x64)

    @test collect(DA32' * Dx64) ≈ A32' * x64
    mul!(Dy, DA32', Dx64)
    @test collect(Dy) ≈ A32' * x64

    B64 = rand(Float64, n, n)
    DB64 = distribute(B64, partA)
    C = DA32 * DB64
    @test C isa Dagger.DMatrix{Float64}
    @test collect(C) ≈ A32 * B64

    DC = distribute(zeros(Float64, n, n), partA)
    mul!(DC, DA32, DB64)
    @test collect(DC) ≈ A32 * B64
    mul!(DC, DA32, DB64, 2.0, 0.0)
    @test collect(DC) ≈ 2 .* (A32 * B64)

    # Wider destination than both operands (FP32 × FP32 → FP64).
    A32b = rand(Float32, n, n)
    DA32b = distribute(A32b, partA)
    Cref = zeros(Float64, n, n)
    mul!(Cref, A32, A32b)
    mul!(DC, DA32, DA32b)
    @test collect(DC) ≈ Cref

    # Opposite mixed: FP64 operator × FP32 vector promotes to FP64.
    A64 = rand(Float64, n, n)
    x32 = rand(Float32, n)
    DA64 = distribute(A64, partA)
    Dx32 = distribute(x32, partV)
    @test collect(DA64 * Dx32) ≈ A64 * x32

    # Sparse tile × dense mixed vector: stay on the SpMV tile kernel.
    SA32 = sprand(Float32, n, n, 0.4)
    DSA32 = distribute(SA32, partA)
    @test collect(DSA32 * Dx64) ≈ SA32 * x64
    mul!(Dy, DSA32, Dx64)
    @test collect(Dy) ≈ SA32 * x64

    # Same-type FP32 GEMM must still agree with host BLAS (the fast path).
    S = rand(Float32, 8, 8)
    Tm = rand(Float32, 8, 8)
    DS = distribute(S, Blocks(4, 4))
    DT = distribute(Tm, Blocks(4, 4))
    @test collect(DS * DT) ≈ S * Tm
    DSt = distribute(zeros(Float32, 8, 8), Blocks(4, 4))
    mul!(DSt, DS, DT)
    @test collect(DSt) ≈ S * Tm
end
