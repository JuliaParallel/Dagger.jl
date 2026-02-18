import Dagger: @stencil, Wrap, Pad, Reflect, Clamp, LinearExtrapolate

function test_stencil()
    @testset "Simple assignment" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil A[idx] = 1
        @test all(collect(A) .== 1)

        # Single expression syntax
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = 2
        @test all(collect(B) .== 2)
    end

    @testset "Neighborhood access of written variable" begin
        A = ones(Blocks(1, 1), Int, 2, 2)
        @stencil A[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        @test all(collect(A) .== 9)
    end

    @testset "Wrap boundary" begin
        A = zeros(Int, 4, 4)
        A[1,1] = 10
        A = DArray(A, Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        # Expected result after convolution with wrap around
        # Corner element (1,1) will sum its 3 neighbors + itself (10) + 5 wrapped around neighbors
        # For A[1,1], neighbors are A[4,4], A[4,1], A[4,2], A[1,4], A[1,2], A[2,4], A[2,1], A[2,2]
        # Since only A[1,1] is 10 and others are 0, sum for B[1,1] will be 10 (A[1,1])
        # Sum for B[1,2] will be A[1,1] = 10
        # Sum for B[2,1] will be A[1,1] = 10
        # Sum for B[2,2] will be A[1,1] = 10
        # Sum for B[4,4] will be A[1,1] = 10
        # ... and so on for elements that wrap around to include A[1,1]
        expected_B_calc = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for ni in -1:1, nj in -1:1
                # Apply wrap around logic for neighbors
                row = mod1(i+ni, 4)
                col = mod1(j+nj, 4)
                if row == 1 && col == 1 # Check if the wrapped neighbor is A[1,1]
                    sum_val += 10
                end
            end
            expected_B_calc[i,j] = sum_val
        end
        @test collect(B) == expected_B_calc
    end

    @testset "Pad boundary" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
        # Expected result after convolution with zero padding
        # Inner elements (e.g., B[2,2]) will sum 9 (3x3 neighborhood of 1s)
        # Edge elements (e.g., B[1,2]) will sum 6 (2x3 neighborhood of 1s, 3 zeros from padding)
        # Corner elements (e.g., B[1,1]) will sum 4 (2x2 neighborhood of 1s, 5 zeros from padding)
        expected_B_pad = [
            4 6 6 4;
            6 9 9 6;
            6 9 9 6;
            4 6 6 4
        ]
        @test collect(B) == expected_B_pad
    end

    @testset "Clamp boundary" begin
        # Test clamping to boundary values
        # For A = [1, 2, 3, 4] with Clamp():
        # idx=0 → 1, idx=-1 → 1 (clamp to first element)
        # idx=5 → 4, idx=6 → 4 (clamp to last element)
        A = DArray([1, 2, 3, 4], Blocks(2))
        B = zeros(Blocks(2), Int, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Clamp()))
        # B[1]: neighbors at indices 0, 1, 2 -> clamped 0 becomes 1, so [1, 1, 2] = 4
        # B[2]: neighbors at indices 1, 2, 3 -> [1, 2, 3] = 6
        # B[3]: neighbors at indices 2, 3, 4 -> [2, 3, 4] = 9
        # B[4]: neighbors at indices 3, 4, 5 -> clamped 5 becomes 4, so [3, 4, 4] = 11
        expected_B_clamp = [4, 6, 9, 11]
        @test collect(B) == expected_B_clamp
    end

    @testset "Clamp boundary 2D" begin
        # Test 2D clamping with a gradient pattern
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Clamp()))
        A_collected = collect(A)
        expected_B_clamp = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                # Apply clamp logic
                ni = clamp(ni, 1, 4)
                nj = clamp(nj, 1, 4)
                sum_val += A_collected[ni, nj]
            end
            expected_B_clamp[i, j] = sum_val
        end
        @test collect(B) == expected_B_clamp
    end

    @testset "LinearExtrapolate boundary" begin
        # Test linear extrapolation using slope at boundary
        # For A = [2.0, 4.0, 6.0, 8.0] with LinearExtrapolate():
        # slope at low boundary = 4.0 - 2.0 = 2.0
        # slope at high boundary = 8.0 - 6.0 = 2.0
        # idx=0 → 2.0 + 2.0*(-1) = 0.0
        # idx=5 → 8.0 + 2.0*(1) = 10.0
        A = DArray([2.0, 4.0, 6.0, 8.0], Blocks(2))
        B = zeros(Blocks(2), Float64, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, LinearExtrapolate()))
        # B[1]: neighbors at indices 0, 1, 2 -> extrapolated 0 becomes 0.0, so [0.0, 2.0, 4.0] = 6.0
        # B[2]: neighbors at indices 1, 2, 3 -> [2.0, 4.0, 6.0] = 12.0
        # B[3]: neighbors at indices 2, 3, 4 -> [4.0, 6.0, 8.0] = 18.0
        # B[4]: neighbors at indices 3, 4, 5 -> extrapolated 5 becomes 10.0, so [6.0, 8.0, 10.0] = 24.0
        expected_B_extrap = [6.0, 12.0, 18.0, 24.0]
        @test collect(B) ≈ expected_B_extrap
    end

    #= FIXME: This takes way too long to run!
    @testset "LinearExtrapolate boundary 2D" begin
        # Test 2D linear extrapolation with a gradient pattern
        A = DArray(Float64.(reshape(1:16, 4, 4)), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Float64, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, LinearExtrapolate()))
        A_collected = collect(A)
        expected_B_extrap = zeros(Float64, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0.0
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                val = 0.0
                # Apply linear extrapolation logic for each dimension
                if ni < 1
                    # Low boundary in dim 1: extrapolate using slope from A[1,:] to A[2,:]
                    base_nj = clamp(nj, 1, 4)
                    slope = A_collected[2, base_nj] - A_collected[1, base_nj]
                    val = A_collected[1, base_nj] + slope * (ni - 1)
                elseif ni > 4
                    # High boundary in dim 1: extrapolate using slope from A[3,:] to A[4,:]
                    base_nj = clamp(nj, 1, 4)
                    slope = A_collected[4, base_nj] - A_collected[3, base_nj]
                    val = A_collected[4, base_nj] + slope * (ni - 4)
                elseif nj < 1
                    # Low boundary in dim 2: extrapolate using slope from A[:,1] to A[:,2]
                    slope = A_collected[ni, 2] - A_collected[ni, 1]
                    val = A_collected[ni, 1] + slope * (nj - 1)
                elseif nj > 4
                    # High boundary in dim 2: extrapolate using slope from A[:,3] to A[:,4]
                    slope = A_collected[ni, 4] - A_collected[ni, 3]
                    val = A_collected[ni, 4] + slope * (nj - 4)
                else
                    val = A_collected[ni, nj]
                end
                sum_val += val
            end
            expected_B_extrap[i, j] = sum_val
        end
        @test collect(B) ≈ expected_B_extrap
    end
    =#

    @testset "Mixed boundary conditions" begin
        # Test different BCs per dimension using a Tuple
        # Use Wrap in dimension 1 and Pad(0) in dimension 2
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, (Wrap(), Pad(0))))
        A_collected = collect(A)
        expected_B_mixed = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                # Dim 1: Wrap
                ni = mod1(i + di, 4)
                # Dim 2: Pad(0)
                nj = j + dj
                if nj < 1 || nj > 4
                    # Padded with 0
                    sum_val += 0
                else
                    sum_val += A_collected[ni, nj]
                end
            end
            expected_B_mixed[i, j] = sum_val
        end
        @test collect(B) == expected_B_mixed
    end

    @testset "Mixed boundary conditions (Clamp, Reflect)" begin
        # Test Clamp in dimension 1 and Reflect(true) in dimension 2
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, (Clamp(), Reflect(true))))
        A_collected = collect(A)
        expected_B_mixed = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                # Dim 1: Clamp
                ni = clamp(i + di, 1, 4)
                # Dim 2: Reflect(true) - symmetric
                nj = j + dj
                nj = nj < 1 ? 1 - nj : (nj > 4 ? 2*4 + 1 - nj : nj)
                sum_val += A_collected[ni, nj]
            end
            expected_B_mixed[i, j] = sum_val
        end
        @test collect(B) == expected_B_mixed
    end

    @testset "Reflect boundary (symmetric)" begin
        # Test symmetric reflection (edge element IS included/repeated)
        # For A = [1, 2, 3, 4] with Reflect(true):
        # idx=0 → 1, idx=-1 → 2 (reflection includes edge)
        # idx=5 → 4, idx=6 → 3 (reflection includes edge)
        A = DArray([1, 2, 3, 4], Blocks(2))
        B = zeros(Blocks(2), Int, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(true)))
        # B[1]: neighbors at indices 0, 1, 2 -> reflected 0 becomes 1, so [1, 1, 2] = 4
        # B[2]: neighbors at indices 1, 2, 3 -> [1, 2, 3] = 6
        # B[3]: neighbors at indices 2, 3, 4 -> [2, 3, 4] = 9
        # B[4]: neighbors at indices 3, 4, 5 -> reflected 5 becomes 4, so [3, 4, 4] = 11
        expected_B_symm = [4, 6, 9, 11]
        @test collect(B) == expected_B_symm
    end

    @testset "Reflect boundary (mirror)" begin
        # Test mirror reflection (edge element NOT included/repeated)
        # For A = [1, 2, 3, 4] with Reflect(false):
        # idx=0 → 2, idx=-1 → 3 (reflection skips edge)
        # idx=5 → 3, idx=6 → 2 (reflection skips edge)
        A = DArray([1, 2, 3, 4], Blocks(2))
        B = zeros(Blocks(2), Int, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(false)))
        # B[1]: neighbors at indices 0, 1, 2 -> reflected 0 becomes 2, so [2, 1, 2] = 5
        # B[2]: neighbors at indices 1, 2, 3 -> [1, 2, 3] = 6
        # B[3]: neighbors at indices 2, 3, 4 -> [2, 3, 4] = 9
        # B[4]: neighbors at indices 3, 4, 5 -> reflected 5 becomes 3, so [3, 4, 3] = 10
        expected_B_mirror = [5, 6, 9, 10]
        @test collect(B) == expected_B_mirror
    end

    @testset "Reflect boundary 2D (symmetric)" begin
        # Test 2D symmetric reflection with a gradient pattern
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(true)))
        # Symmetric: idx < 1 → 1 - idx, idx > size → 2*size + 1 - idx
        A_collected = collect(A)
        expected_B_symm = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                # Apply symmetric reflection logic
                # For symmetric: idx < 1 → 1 - idx, idx > size → 2*size + 1 - idx
                ni = ni < 1 ? 1 - ni : (ni > 4 ? 2*4 + 1 - ni : ni)
                nj = nj < 1 ? 1 - nj : (nj > 4 ? 2*4 + 1 - nj : nj)
                sum_val += A_collected[ni, nj]
            end
            expected_B_symm[i, j] = sum_val
        end
        @test collect(B) == expected_B_symm
    end

    @testset "Reflect boundary 2D (mirror)" begin
        # Test 2D mirror reflection with a gradient pattern
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(false)))
        # Mirror: idx < 1 → 2 - idx, idx > size → 2*size - idx
        A_collected = collect(A)
        expected_B_mirror = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                # Apply mirror reflection logic
                ni = ni < 1 ? 2 - ni : (ni > 4 ? 2*4 - ni : ni)
                nj = nj < 1 ? 2 - nj : (nj > 4 ? 2*4 - nj : nj)
                sum_val += A_collected[ni, nj]
            end
            expected_B_mirror[i, j] = sum_val
        end
        @test collect(B) == expected_B_mirror
    end

    @testset "Multiple expressions" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil begin
            A[idx] = 1
            B[idx] = A[idx] * 2
        end
        expected_A_multi = [1 for r in 1:4, c in 1:4]
        expected_B_multi = expected_A_multi .* 2
        @test collect(A) == expected_A_multi
        @test collect(B) == expected_B_multi
    end

    @testset "Allocation syntax" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        B = @stencil sum(@neighbors(A[idx], 1, Wrap()))
        @test B isa DArray
        @test all(collect(B) .== 9)

        C = @stencil begin
            A[idx] = A[idx] + 1
            sum(@neighbors(A[idx], 1, Wrap()))
        end
        @test C isa DArray
        @test all(collect(C) .== 18)
    end

    @testset "Broadcast integration" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        B = ones(Blocks(2, 2), Int, 4, 4)
        C = zeros(Blocks(2, 2), Int, 4, 4)

        # Test that @stencil can be used in a broadcast expression
        C .= A .+ @stencil(sum(@neighbors(B[idx], 1, Wrap())))

        # sum(@neighbors(B[idx], 1, Wrap())) should be 9 everywhere if B is all ones
        # A is all ones
        # C should be 1 + 9 = 10 everywhere
        @test all(collect(C) .== 10)
    end

    @testset "Multiple DArrays" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        B = DArray(fill(2, 4, 4), Blocks(2, 2))
        C = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil C[idx] = A[idx] + B[idx]
        @test all(collect(C) .== 3)
    end

    @testset "Update operators" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        @stencil A[idx] += 1
        @test all(collect(A) .== 2)

        B = ones(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] *= 3
        @test all(collect(B) .== 3)

        C = DArray(fill(10.0, 4, 4), Blocks(2, 2))
        @stencil C[idx] /= 2.0
        @test all(collect(C) .== 5.0)

        D = DArray(fill(10, 4, 4), Blocks(2, 2))
        @stencil D[idx] -= 1
        @test all(collect(D) .== 9)

        E = ones(Blocks(2, 2), Int, 4, 4)
        @stencil E[idx] += sum(@neighbors(E[idx], 1, Wrap()))
        # E initially all 1s.
        # Neighborhood sum is 9.
        # E[idx] = E[idx] + 9 = 1 + 9 = 10
        @test all(collect(E) .== 10)
    end

    @testset "Pad boundary with non-zero value" begin
        A = ones(Blocks(1, 1), Int, 2, 2) # Simpler 2x2 case
        B = zeros(Blocks(1, 1), Int, 2, 2)
        pad_value = 5
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Pad(pad_value)))
        # For A = [1 1; 1 1] and Pad(5)
        # B[1,1] neighbors considering a 3x3 neighborhood around A[1,1]:
        # P P P
        # P A11 A12
        # P A21 A22
        # Values:
        # 5 5 5
        # 5 1 1
        # 5 1 1
        # Sum = 5*5 (for the padded values) + 1*4 (for the actual values from A) = 25 + 4 = 29.
        # This logic applies to all elements in B because the array A is small (2x2) and the neighborhood is 1.
        # Every element's 3x3 neighborhood will include 5 padded values and the 4 values of A.
        expected_B_pad_val = fill(pad_value*5 + 1*4, 2, 2)
        @test collect(B) == expected_B_pad_val
    end

    # From issue #669
    for N in 3:4
        @testset "$(N)D array" begin
            A = ones(Blocks(ntuple(_->1, N)...), Int, ntuple(_->3, N)...)
            Dagger.allowscalar() do
                A[:] = 1:length(A)
            end
            B = zeros(Blocks(ntuple(_->1, N)...), Float32, ntuple(_->3, N)...)

            @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap())) / length(A)
            @test all(==(Float64(sum(1:length(A)) / length(A))), collect(B))
        end
    end

    @testset "Tuple neighborhood distance" begin
        # 1D case: distance (2,)
        @testset "1D with distance (2,)" begin
            A = DArray([1, 2, 3, 4, 5, 6], Blocks(2,))
            B = zeros(Blocks(2,), Int, 6)
            @stencil B[idx] = sum(@neighbors(A[idx], (2,), Wrap()))
            # For each element, neighbors at distance 2 in 1D: [-2, -1, 0, 1, 2]
            # B[1] neighbors: A[5], A[6], A[1], A[2], A[3] (wrapping) = 5+6+1+2+3 = 17
            # B[2] neighbors: A[6], A[1], A[2], A[3], A[4] = 6+1+2+3+4 = 16
            # B[3] neighbors: A[1], A[2], A[3], A[4], A[5] = 1+2+3+4+5 = 15
            # B[4] neighbors: A[2], A[3], A[4], A[5], A[6] = 2+3+4+5+6 = 20
            # B[5] neighbors: A[3], A[4], A[5], A[6], A[1] = 3+4+5+6+1 = 19
            # B[6] neighbors: A[4], A[5], A[6], A[1], A[2] = 4+5+6+1+2 = 18
            expected_B_1d = [17, 16, 15, 20, 19, 18]
            @test collect(B) == expected_B_1d
        end

        # 2D case: distance (1, 2) - different per dimension
        @testset "2D with distance (1, 2)" begin
            A = DArray(reshape(1:12, 3, 4), Blocks(1, 2))
            B = zeros(Blocks(1, 2), Int, 3, 4)
            @stencil B[idx] = sum(@neighbors(A[idx], (1, 2), Wrap()))
            # Distance (1, 2) means:
            # - dimension 1 (rows): offsets -1, 0, 1
            # - dimension 2 (cols): offsets -2, -1, 0, 1, 2
            # Total neighborhood size: 3 * 5 = 15 elements
            expected_B_2d = zeros(Int, 3, 4)
            for i in 1:3, j in 1:4
                sum_val = 0
                for di in -1:1, dj in -2:2
                    row = mod1(i+di, 3)
                    col = mod1(j+dj, 4)
                    sum_val += A[row, col]
                end
                expected_B_2d[i, j] = sum_val
            end
            @test collect(B) == expected_B_2d
        end

        # 3D case: distance (1, 2, 1) - different per dimension
        @testset "3D with distance (1, 2, 1)" begin
            # Need chunk sizes >= 2*distance+1 for each dimension
            # distance (1, 2, 1) requires chunks >= (3, 5, 3)
            A = DArray(reshape(1:120, 4, 5, 6), Blocks(4, 5, 3))
            B = zeros(Blocks(4, 5, 3), Int, 4, 5, 6)
            @stencil B[idx] = sum(@neighbors(A[idx], (1, 2, 1), Wrap()))
            # Distance (1, 2, 1) means:
            # - dimension 1: offsets -1, 0, 1 (3 elements)
            # - dimension 2: offsets -2, -1, 0, 1, 2 (5 elements)
            # - dimension 3: offsets -1, 0, 1 (3 elements)
            # Total neighborhood size: 3 * 5 * 3 = 45 elements
            expected_B_3d = zeros(Int, 4, 5, 6)
            for i in 1:4, j in 1:5, k in 1:6
                sum_val = 0
                for di in -1:1, dj in -2:2, dk in -1:1
                    row = mod1(i+di, 4)
                    col = mod1(j+dj, 5)
                    depth = mod1(k+dk, 6)
                    sum_val += A[row, col, depth]
                end
                expected_B_3d[i, j, k] = sum_val
            end
            @test collect(B) == expected_B_3d
        end
    end

    @testset "Invalid neighborhood distance" begin
        for value in [0, -1, 1.5, 2]
            for dist in [value, (value,)]
                @test_throws_unwrap ArgumentError @eval begin
                    A = ones(Blocks(1, 1), Int, 2, 2)
                    B = zeros(Blocks(1, 1), Int, 2, 2)
                    @stencil B[idx] = sum(@neighbors(A[idx], $dist, Wrap()))
                end
            end
        end
    end

    #= FIXME: Can't detect this anymore, because we allow arbitrary expressions in @stencil
    @testset "Invalid update expression" begin
        @test_throws_unwrap ArgumentError @eval begin
            A = ones(Blocks(1, 1), Int, 2, 2)
            @stencil A[idx] += 1
        end
    end
    =#
end

@testset "CPU" begin
    test_stencil()
end

@testset "GPU" begin
    for (kind, scope) in GPU_SCOPES
        # FIXME
        kind == :oneAPI && continue
        @testset "$kind" begin
            Dagger.with_options(;scope) do
                test_stencil()
            end
        end
    end
end

