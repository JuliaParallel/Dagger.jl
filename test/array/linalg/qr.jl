 @testset "Tile QR:  $T" for T in (Float32, Float64, ComplexF32, ComplexF64)
     ## Square matrices
     A = rand(T, 128, 128)
     Q, R = qr(A)
     DA = distribute(A, Blocks(32,32))
     DQ, DR = qr!(DA)
     @test abs.(DR) ≈ abs.(R)
     @test DQ * DR ≈ A
     @test DQ' * DQ ≈ I
     @test I * DQ ≈ collect(DQ)
     @test I * DQ' ≈ collect(DQ')
     @test triu(collect(DR)) ≈ collect(DR)
     ## Rectangular matrices (block and element wise)
     # Tall Element and Block
     A = rand(T, 128, 64)
     Q, R = qr(A)
     DA = distribute(A, Blocks(32,32))
     DQ, DR = qr!(DA)
     @test abs.(DR) ≈ abs.(R)
     @test DQ * DR ≈ A
     @test DQ' * DQ ≈ I
     @test I * DQ ≈ collect(DQ)
     @test I * DQ' ≈ collect(DQ')
     @test triu(collect(DR)) ≈ collect(DR)
 
     # Wide Element and Block
     A = rand(T, 64, 128)
     Q, R = qr(A)
     DA = distribute(A, Blocks(16,16))
     DQ, DR = qr!(DA)
     @test abs.(DR) ≈ abs.(R)
     @test DQ * DR ≈ A
     @test DQ' * DQ ≈ I
     @test I * DQ ≈ collect(DQ)
     @test I * DQ' ≈ collect(DQ')
     @test triu(collect(DR)) ≈ collect(DR)
end
