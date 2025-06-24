import Dagger: @stencil, Wrap, Pad

@testset "@stencil" begin
    @testset "Simple assignment" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.spawn_datadeps() do
            @stencil begin
                A[idx] = 1
            end
        end
        @test all(collect(A) .== 1)
    end

    @testset "Wrap boundary" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        A[1,1] = 10
        B = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.spawn_datadeps() do
            @stencil begin
                B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
            end
        end
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
        A = DArray(ones(Int, 4, 4), Blocks(2, 2))
        B = DArray(zeros(Int, 4, 4), Blocks(2, 2))
        Dagger.spawn_datadeps() do
            @stencil begin
                B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
            end
        end
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

    @testset "Multiple expressions" begin
        A = zeros(Blocks(2, 2), Int, 4, 4)
        B = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.spawn_datadeps() do
            @stencil begin
                A[idx] = 1
                B[idx] = A[idx] * 2
            end
        end
        expected_A_multi = [1 for r in 1:4, c in 1:4]
        expected_B_multi = expected_A_multi .* 2
        @test collect(A) == expected_A_multi
        @test collect(B) == expected_B_multi
    end

    @testset "Multiple DArrays" begin
        A = ones(Blocks(2, 2), Int, 4, 4)
        B = DArray(fill(2, 4, 4), Blocks(2, 2))
        C = zeros(Blocks(2, 2), Int, 4, 4)
        Dagger.spawn_datadeps() do
            @stencil begin
                C[idx] = A[idx] + B[idx]
            end
        end
        @test all(collect(C) .== 3)
    end

    @testset "Pad boundary with non-zero value" begin
        A = ones(Blocks(1, 1), Int, 2, 2) # Simpler 2x2 case
        B = zeros(Blocks(1, 1), Int, 2, 2)
        pad_value = 5
        Dagger.spawn_datadeps() do
            @stencil begin
                B[idx] = sum(@neighbors(A[idx], 1, Pad(pad_value)))
            end
        end
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
end
