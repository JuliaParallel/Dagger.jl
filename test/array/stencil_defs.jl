# Shared `@stencil` correctness suite. Defines `test_stencil` so it can be
# reused both for the single-process CPU/GPU testsets below and for the
# MPI(+GPU) suites in test/mpi.jl / test/mpi_gpu_suite.jl.

@everywhere import Dagger: @stencil, Wrap, Pad, Reflect, AntiReflect, Clamp, LinearExtrapolate
# For `test_stencil_sparse` below; workers allocate the sparse tiles, so they
# need `SparseArrays` loaded for `Dagger`'s SparseArrays extension to be active.
@everywhere using SparseArrays
using Random

function test_stencil(; skip_highdim::Bool=false)
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
        A = DArray([2f0, 4f0, 6f0, 8f0], Blocks(2))
        B = zeros(Blocks(2), Float32, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, LinearExtrapolate()))
        # B[1]: neighbors at indices 0, 1, 2 -> extrapolated 0 becomes 0.0, so [0.0, 2.0, 4.0] = 6.0
        # B[2]: neighbors at indices 1, 2, 3 -> [2.0, 4.0, 6.0] = 12.0
        # B[3]: neighbors at indices 2, 3, 4 -> [4.0, 6.0, 8.0] = 18.0
        # B[4]: neighbors at indices 3, 4, 5 -> extrapolated 5 becomes 10.0, so [6.0, 8.0, 10.0] = 24.0
        expected_B_extrap = [6f0, 12f0, 18f0, 24f0]
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

    @testset "AntiReflect constructor" begin
        # AntiReflect() is the symmetric variant, matching Reflect(true)'s indexing
        @test AntiReflect() === AntiReflect(true)
        @test AntiReflect(false) !== AntiReflect(true)
    end

    @testset "AntiReflect ghost values" begin
        # The defining property: an out-of-domain neighbor is the *negated*
        # reflected value, so the field interpolates to zero at the boundary.
        A = DArray([1, 2, 3, 4], Blocks(2))
        lo = zeros(Blocks(2), Int, 4)
        hi = zeros(Blocks(2), Int, 4)
        @stencil begin
            lo[idx] = @neighbors(A[idx], 1, AntiReflect(true))[1]
            hi[idx] = @neighbors(A[idx], 1, AntiReflect(true))[3]
        end
        # lo[1] and hi[4] are the ghosts: -A[1] and -A[4]
        @test collect(lo) == [-1, 1, 2, 3]
        @test collect(hi) == [2, 3, 4, -4]
    end

    @testset "AntiReflect boundary (symmetric)" begin
        # For A = [1, 2, 3, 4] with AntiReflect(true):
        # idx=0 → -A[1] = -1, idx=5 → -A[4] = -4 (Reflect(true) indexing, negated)
        A = DArray([1, 2, 3, 4], Blocks(2))
        B = zeros(Blocks(2), Int, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, AntiReflect(true)))
        # B[1]: indices 0, 1, 2 -> [-1, 1, 2] = 2
        # B[2]: indices 1, 2, 3 -> [1, 2, 3] = 6
        # B[3]: indices 2, 3, 4 -> [2, 3, 4] = 9
        # B[4]: indices 3, 4, 5 -> [3, 4, -4] = 3
        @test collect(B) == [2, 6, 9, 3]
    end

    @testset "AntiReflect boundary (mirror)" begin
        # For A = [1, 2, 3, 4] with AntiReflect(false):
        # idx=0 → -A[2] = -2, idx=5 → -A[3] = -3 (Reflect(false) indexing, negated)
        A = DArray([1, 2, 3, 4], Blocks(2))
        B = zeros(Blocks(2), Int, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, AntiReflect(false)))
        # B[1]: indices 0, 1, 2 -> [-2, 1, 2] = 1
        # B[2]: indices 1, 2, 3 -> [1, 2, 3] = 6
        # B[3]: indices 2, 3, 4 -> [2, 3, 4] = 9
        # B[4]: indices 3, 4, 5 -> [3, 4, -3] = 4
        @test collect(B) == [1, 6, 9, 4]
    end

    @testset "AntiReflect boundary 2D (symmetric)" begin
        # Applied to every dimension, the sign flips once per reflected
        # dimension, so corner regions (reflected twice) keep their sign.
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, AntiReflect(true)))
        A_collected = collect(A)
        expected_B = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                sgn = 1
                if ni < 1
                    ni = 1 - ni; sgn = -sgn
                elseif ni > 4
                    ni = 2*4 + 1 - ni; sgn = -sgn
                end
                if nj < 1
                    nj = 1 - nj; sgn = -sgn
                elseif nj > 4
                    nj = 2*4 + 1 - nj; sgn = -sgn
                end
                sum_val += sgn * A_collected[ni, nj]
            end
            expected_B[i, j] = sum_val
        end
        @test collect(B) == expected_B
    end

    @testset "Mixed boundary conditions (Wrap, AntiReflect)" begin
        # The solid-wall case: periodic along dimension 1, walled along
        # dimension 2, for a quantity normal to that wall.
        A = DArray(reshape(1:16, 4, 4), Blocks(2, 2))
        B = zeros(Blocks(2, 2), Int, 4, 4)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, (Wrap(), AntiReflect(true))))
        A_collected = collect(A)
        expected_B = zeros(Int, 4, 4)
        for i in 1:4, j in 1:4
            sum_val = 0
            for di in -1:1, dj in -1:1
                # Dim 1: Wrap
                ni = mod1(i + di, 4)
                # Dim 2: AntiReflect(true) - symmetric indices, negated values
                nj = j + dj
                sgn = 1
                if nj < 1
                    nj = 1 - nj; sgn = -1
                elseif nj > 4
                    nj = 2*4 + 1 - nj; sgn = -1
                end
                sum_val += sgn * A_collected[ni, nj]
            end
            expected_B[i, j] = sum_val
        end
        @test collect(B) == expected_B
    end

    @testset "AntiReflect zeroes the flux through a wall" begin
        # Why this boundary condition exists: for a field normal to a solid
        # wall, the centered difference across the wall must see a ghost that
        # cancels the edge value. With a uniform field the wall-adjacent
        # centered difference is therefore non-zero (the flow is stopped),
        # while Reflect leaves it at zero (the flow passes straight through).
        A = ones(Blocks(2), Float32, 4)
        anti = zeros(Blocks(2), Float32, 4)
        refl = zeros(Blocks(2), Float32, 4)
        @stencil begin
            anti[idx] = begin
                n = @neighbors(A[idx], 1, AntiReflect())
                n[3] - n[1]
            end
            refl[idx] = begin
                n = @neighbors(A[idx], 1, Reflect(true))
                n[3] - n[1]
            end
        end
        # Interior cells see 1 - 1 == 0 under both conditions
        @test collect(anti)[2:3] == [0.0f0, 0.0f0]
        @test collect(refl) == zeros(4)
        # At the edges the antireflected ghost is -1, so the difference is ±2
        @test collect(anti)[1] == 2.0f0
        @test collect(anti)[4] == -2.0f0
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

        C = DArray(fill(10f0, 4, 4), Blocks(2, 2))
        @stencil C[idx] /= 2f0
        @test all(collect(C) .== 5f0)

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
    # N.B. The Metal backend currently breaks on these higher-dimensional
    # stencils and corrupts subsequent tests, so they can be skipped there.
    if !skip_highdim
        for N in 3:4
            @testset "$(N)D array" begin
                A = ones(Blocks(ntuple(_->1, N)...), Int, ntuple(_->3, N)...)
                Dagger.allowscalar() do
                    A[:] = 1:length(A)
                end
                B = zeros(Blocks(ntuple(_->1, N)...), Float32, ntuple(_->3, N)...)

                @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap())) ÷ length(A)
                @test all(==(Float64(sum(1:length(A)) / length(A))), collect(B))
            end
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

# `@stencil` on sparse `DArray`s whose tiles are device-resident.
#
# Two paths, and this checks both against the same stencil over a dense CPU
# `DArray`:
#
#   - sparse operands, dense output: runs entirely on the device. The operand is
#     read through `DeviceSparseMatrixCSC`'s device-side `getindex`, and the halo
#     regions -- thin next to the center -- are materialized dense so the kernel
#     can index them.
#   - sparse output: no kernel can insert a nonzero into a device CSC, so the
#     sweep stages through host storage and the result is uploaded again. The
#     tile must still come back device-resident, which is the last check here.
#
# `scope` selects the backend; the caller has already entered it.
function test_stencil_sparse_gpu()
    n, blk = 16, 8
    part = Blocks(blk, blk)
    mkmat() = (Random.seed!(1234); SparseArrays.sprand(Float64, n, n, 0.25))

    # Reference: the same stencils over dense CPU tiles.
    dense_ref(boundary) = begin
        Ad = distribute(Array(mkmat()), part)
        D = zeros(part, Float64, n, n)
        @stencil D[idx] = sum(@neighbors(Ad[idx], 1, boundary))
        collect(D)
    end

    @testset "sparse operands, dense output: $(nameof(typeof(boundary)))" for boundary in
            (Wrap(), Pad(0.0), Pad(1.5), Clamp(), Reflect(true), AntiReflect(true),
             LinearExtrapolate())
        A = distribute(mkmat(), part)
        D = zeros(part, Float64, n, n)
        @stencil D[idx] = sum(@neighbors(A[idx], 1, boundary))
        @test collect(D) ≈ dense_ref(boundary)
    end

    @testset "sparse operands, dense output: distance 2" begin
        A = distribute(mkmat(), part)
        D = zeros(part, Float64, n, n)
        @stencil D[idx] = sum(@neighbors(A[idx], 2, Wrap()))

        Ad = distribute(Array(mkmat()), part)
        R = zeros(part, Float64, n, n)
        @stencil R[idx] = sum(@neighbors(Ad[idx], 2, Wrap()))
        @test collect(D) ≈ collect(R)
    end

    @testset "sparse output stages through the host" begin
        A = distribute(mkmat(), part)
        C = SparseArrays.spzeros(part, Float64, n, n)
        @stencil C[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
        @test collect(C) ≈ dense_ref(Pad(0.0))

        # ... and with the dilated sweep, which the host staging also has to honor.
        C2 = SparseArrays.spzeros(part, Float64, n, n)
        @stencil sparse=true C2[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
        @test collect(C2) ≈ dense_ref(Pad(0.0))
    end

    @testset "sparse output stays device-resident" begin
        # A host-staged sweep that forgot to upload its result would leave the
        # tile on the host, and the next sweep would silently run on the CPU.
        tile_spaces(D) = unique(map(Dagger.chunks(D)) do c
            fetch(Dagger.@spawn (x -> Dagger.value_memory_space(x.mat))(c))
        end)

        A = distribute(mkmat(), part)
        C = SparseArrays.spzeros(part, Float64, n, n)
        want = tile_spaces(C)
        @stencil C[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
        @test tile_spaces(C) == want

        # Chain a second sweep off the first one's output.
        @stencil C[idx] = sum(@neighbors(C[idx], 1, Pad(0.0)))
        @test tile_spaces(C) == want

        Aref = distribute(mkmat(), part)
        R = SparseArrays.spzeros(part, Float64, n, n)
        Dagger.with_options(scope=Dagger.scope(worker=1, thread=1)) do
            @stencil R[idx] = sum(@neighbors(Aref[idx], 1, Pad(0.0)))
            @stencil R[idx] = sum(@neighbors(R[idx], 1, Pad(0.0)))
        end
        @test collect(C) ≈ collect(R)
    end
end

#############################################################################
# Sparse tiles
#############################################################################

# `@stencil` on sparse `DArray`s (host `SparseMatrixCSC` / `SparseVector` tiles).
#
# Nothing in the stencil machinery is sparse-aware: this works because a
# `DSparseArray` tile forwards `size`/`getindex`/`setindex!`/`similar`/`view` to
# its storage, and `stencil_storage` unwraps it so each sweep specializes on that
# storage. The suite is therefore mostly a *differential* test -- every result is
# compared against the same stencil over a dense `DArray` built from the same
# matrix, which is the property that would break if any of those forwards were
# lost.
#
# N.B. CPU only. GPU sparse tiles have no device-side `setindex!` (inserting a
# nonzero is a structural change), so they cannot be swept by a kernel at all;
# `test/array/stencil.jl` calls this from the CPU testset only.
function test_stencil_sparse()
    # Every array is built from a seeded RNG so the dense reference and the
    # sparse operand are the same matrix.
    mkpair(part, dims, density) = begin
        Random.seed!(1234)
        S = SparseArrays.sprand(Float64, dims..., density)
        (distribute(S, part), distribute(Array(S), part))
    end

    # Storage actually reachable under the `DSparseArray` wrapper, per tile.
    tile_storage(D) = unique(map(c -> typeof(fetch(c).mat), Dagger.chunks(D)))
    stored_nnz(D) = sum(c -> SparseArrays.nnz(fetch(c).mat), Dagger.chunks(D))

    @testset "Tiles stay sparse" begin
        part = Blocks(4, 4)
        A, _ = mkpair(part, (8, 8), 0.3)
        @test tile_storage(A) == [SparseArrays.SparseMatrixCSC{Float64,Int}]

        B = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        # The sweep must write through the wrapper into CSC storage, not
        # replace the tile with a dense array.
        @test tile_storage(B) == [SparseArrays.SparseMatrixCSC{Float64,Int}]
        # A 3x3 stencil dilates the support, so the result is denser than the
        # input but must still not be structurally full for this density.
        @test stored_nnz(B) > stored_nnz(A)
        @test stored_nnz(B) < 8 * 8
    end

    @testset "$(nameof(typeof(boundary))) boundary matches dense" for boundary in
            (Wrap(), Pad(0.0), Pad(1.5), Clamp(), Reflect(true), Reflect(false),
             AntiReflect(true), LinearExtrapolate())
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = sum(@neighbors(A[idx], 1, boundary))
        D = zeros(part, Float64, 8, 8)
        @stencil D[idx] = sum(@neighbors(Ad[idx], 1, boundary))

        @test collect(S) ≈ collect(D)
    end

    @testset "Mixed boundary conditions" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)
        boundary = (Wrap(), Pad(0.0))

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = sum(@neighbors(A[idx], 1, boundary))
        D = zeros(part, Float64, 8, 8)
        @stencil D[idx] = sum(@neighbors(Ad[idx], 1, boundary))

        @test collect(S) ≈ collect(D)
    end

    @testset "Tuple neighborhood distance" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = sum(@neighbors(A[idx], (1, 2), Wrap()))
        D = zeros(part, Float64, 8, 8)
        @stencil D[idx] = sum(@neighbors(Ad[idx], (1, 2), Wrap()))

        @test collect(S) ≈ collect(D)
    end

    @testset "Elementwise (no neighborhood)" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = A[idx] + 1
        @test collect(S) ≈ collect(Ad) .+ 1
        # Adding a constant fills the tile in structurally; the result must
        # still be correct, and still be sparse *storage*.
        @test tile_storage(S) == [SparseArrays.SparseMatrixCSC{Float64,Int}]
    end

    @testset "Write back into the array being read" begin
        # Exercises `stencil_source_chunks`, which snapshots the read chunks
        # before any is overwritten -- `copy` of a sparse tile, not a dense one.
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        @stencil A[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        @stencil Ad[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))

        @test collect(A) ≈ collect(Ad)
        @test tile_storage(A) == [SparseArrays.SparseMatrixCSC{Float64,Int}]
    end

    @testset "Multiple expressions" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)
        S = SparseArrays.spzeros(part, Float64, 8, 8)
        D = zeros(part, Float64, 8, 8)

        @stencil begin
            S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
            S[idx] = S[idx] * 2
        end
        @stencil begin
            D[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))
            D[idx] = D[idx] * 2
        end

        @test collect(S) ≈ collect(D)
    end

    @testset "Allocation syntax" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        S = @stencil sum(@neighbors(A[idx], 1, Wrap()))
        D = @stencil sum(@neighbors(Ad[idx], 1, Wrap()))

        @test S isa DArray
        # `similar` on a sparse DArray must carry the tile's backend forward,
        # otherwise the allocated result would be dense.
        @test tile_storage(S) == [SparseArrays.SparseMatrixCSC{Float64,Int}]
        @test collect(S) ≈ collect(D)
    end

    @testset "Mixed sparse and dense operands" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        # Sparse in, dense out.
        D = zeros(part, Float64, 8, 8)
        @stencil D[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        Dref = zeros(part, Float64, 8, 8)
        @stencil Dref[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))
        @test collect(D) ≈ collect(Dref)

        # Sparse and dense read in the same expression.
        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = sum(@neighbors(A[idx], 1, Wrap())) + Ad[idx]
        @test collect(S) ≈ collect(Dref) .+ collect(Ad)
    end

    @testset "1D sparse vector" begin
        part = Blocks(8)
        Random.seed!(1234)
        v = SparseArrays.sprand(Float64, 16, 0.3)
        A = distribute(v, part)
        Ad = distribute(Array(v), part)

        S = SparseArrays.spzeros(part, Float64, 16)
        @stencil S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        D = zeros(part, Float64, 16)
        @stencil D[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))

        @test collect(S) ≈ collect(D)
    end

    @testset "Uneven partitioning" begin
        # Blocks that do not divide the array evenly, so tiles differ in size.
        part = Blocks(3, 3)
        A, Ad = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        D = zeros(part, Float64, 8, 8)
        @stencil D[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))

        @test collect(S) ≈ collect(D)
    end

    #########################################################################
    # `sparse=true`: the dilated-support sweep
    #########################################################################

    # `sparse=true` restricts the sweep to indices where the result can be
    # nonzero. For a zero-preserving kernel that must be *indistinguishable*
    # from the default sweep -- not merely close, but the same stored pattern
    # and the same values -- which is what these compare. Anything the
    # candidate set wrongly excludes shows up as a missing stored entry.
    same_tiles(X, Y) = all(zip(Dagger.chunks(X), Dagger.chunks(Y))) do (cx, cy)
        x, y = fetch(cx).mat, fetch(cy).mat
        x.colptr == y.colptr && x.rowval == y.rowval && x.nzval == y.nzval
    end

    @testset "Option parsing" begin
        @test Dagger.parse_stencil_options(()) === Dagger.DenseSweep()
        @test Dagger.parse_stencil_options((:(sparse = false),)) === Dagger.DenseSweep()
        @test Dagger.parse_stencil_options((:(sparse = true),)) === Dagger.SparseSweep()
        # Not a literal Bool, so the sweep style could not be decided at expansion.
        @test_throws ArgumentError Dagger.parse_stencil_options((:(sparse = 1),))
        @test_throws ArgumentError Dagger.parse_stencil_options((:(bogus = true),))
        @test_throws ArgumentError Dagger.parse_stencil_options((:(notanoption),))
    end

    @testset "sparse=true matches the dense sweep: $(nameof(typeof(boundary)))" for boundary in
            (Wrap(), Pad(0.0), Clamp(), Reflect(true), Reflect(false),
             AntiReflect(true), LinearExtrapolate())
        # N.B. `Pad(v)` with `v != 0` is deliberately absent: it is not
        # zero-preserving, so `sparse=true` is documented to be wrong for it.
        part = Blocks(4, 4)
        A, _ = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, boundary))
        R = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, boundary))

        @test same_tiles(S, R)
    end

    @testset "sparse=true, larger tiles" begin
        # 8x8 tiles are almost entirely boundary shell; this size actually
        # exercises the interior of the dilated pattern.
        part = Blocks(32, 32)
        A, _ = mkpair(part, (64, 64), 0.02)

        S = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
        R = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))

        @test same_tiles(S, R)
        # The point of the exercise: the result is dilated but still sparse.
        @test stored_nnz(S) < 64 * 64
    end

    @testset "sparse=true, neighborhood distance 2" begin
        part = Blocks(32, 32)
        A, _ = mkpair(part, (64, 64), 0.02)

        S = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 2, Wrap()))
        R = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil R[idx] = sum(@neighbors(A[idx], 2, Wrap()))

        @test same_tiles(S, R)
    end

    @testset "sparse=true, tuple neighborhood distance" begin
        # Anisotropic dilation: rows by 1, columns by 2.
        part = Blocks(32, 32)
        A, _ = mkpair(part, (64, 64), 0.02)

        S = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], (1, 2), Wrap()))
        R = SparseArrays.spzeros(part, Float64, 64, 64)
        @stencil R[idx] = sum(@neighbors(A[idx], (1, 2), Wrap()))

        @test same_tiles(S, R)
    end

    @testset "sparse=true, multiple sparse operands" begin
        part = Blocks(4, 4)
        A, _ = mkpair(part, (8, 8), 0.3)
        Random.seed!(4321)
        B = distribute(SparseArrays.sprand(Float64, 8, 8, 0.2), part)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, Pad(0.0))) + B[idx]
        R = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, Pad(0.0))) + B[idx]

        @test same_tiles(S, R)
    end

    @testset "sparse=true, output-only follow-up expression" begin
        # The second expression reads nothing but the output, so its candidate
        # set is the output's own pattern rather than a dilated one.
        part = Blocks(4, 4)
        A, _ = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil sparse=true begin
            S[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
            S[idx] = S[idx] * 2
        end
        R = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil begin
            R[idx] = sum(@neighbors(A[idx], 1, Pad(0.0)))
            R[idx] = R[idx] * 2
        end

        @test same_tiles(S, R)
    end

    @testset "sparse=true, write back into the array being read" begin
        part = Blocks(4, 4)
        A, _ = mkpair(part, (8, 8), 0.3)
        R, _ = mkpair(part, (8, 8), 0.3)

        @stencil sparse=true A[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        @stencil R[idx] = sum(@neighbors(R[idx], 1, Wrap()))

        @test same_tiles(A, R)
    end

    @testset "sparse=true, 1D sparse vector" begin
        part = Blocks(8)
        Random.seed!(1234)
        v = SparseArrays.sprand(Float64, 16, 0.3)
        A = distribute(v, part)

        S = SparseArrays.spzeros(part, Float64, 16)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        R = SparseArrays.spzeros(part, Float64, 16)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, Wrap()))

        @test collect(S) == collect(R)
    end

    @testset "sparse=true, uneven partitioning" begin
        part = Blocks(3, 3)
        A, _ = mkpair(part, (8, 8), 0.3)

        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, Wrap()))
        R = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, Wrap()))

        @test same_tiles(S, R)
    end

    @testset "sparse=true falls back where it does not apply" begin
        part = Blocks(4, 4)
        A, Ad = mkpair(part, (8, 8), 0.3)

        # Dense output: no sparse-aware sweep, so the dense one runs.
        D = zeros(part, Float64, 8, 8)
        @stencil sparse=true D[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))
        Dref = zeros(part, Float64, 8, 8)
        @stencil Dref[idx] = sum(@neighbors(Ad[idx], 1, Wrap()))
        @test collect(D) == collect(Dref)

        # A dense operand can be nonzero anywhere, so there is no pattern to
        # dilate even though the output is sparse.
        S = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil sparse=true S[idx] = sum(@neighbors(A[idx], 1, Pad(0.0))) + Ad[idx]
        R = SparseArrays.spzeros(part, Float64, 8, 8)
        @stencil R[idx] = sum(@neighbors(A[idx], 1, Pad(0.0))) + Ad[idx]
        @test same_tiles(S, R)
    end
end
