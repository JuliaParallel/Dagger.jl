using Test
using Dagger: ReusableLinkedList

@testset "ReusableLinkedList" begin
    @testset "Construction and basic operations" begin
        null_value = 0
        list = ReusableLinkedList{Int}(null_value, 10)

        @test length(list) == 0
        @test isempty(list)

        push!(list, 1)
        @test length(list) == 1
        @test !isempty(list)
        @test list[1] == 1

        push!(list, 2)
        @test length(list) == 2
        @test list[2] == 2

        @test pop!(list) == 2
        @test length(list) == 1
        @test list[1] == 1

        @test_throws BoundsError list[2]
        @test_throws BoundsError list[0]
    end

    @testset "Iteration" begin
        list = ReusableLinkedList{Int}(0, 5)
        values = [1, 2, 3, 4, 5]
        for v in values
            push!(list, v)
        end

        @test collect(list) == values
        @test [x for x in list] == values
    end

    @testset "Indexing and modification" begin
        list = ReusableLinkedList{Int}(0, 5)
        push!(list, 1)
        push!(list, 2)
        push!(list, 3)

        @test list[2] == 2
        list[2] = 20
        @test list[2] == 20
        @test collect(list) == [1, 20, 3]

        @test_throws BoundsError list[4] = 4
    end

    @testset "Empty and refill" begin
        list = ReusableLinkedList{Int}(0, 5)
        for i in 1:5
            push!(list, i)
        end

        @test length(list) == 5
        empty!(list)
        @test isempty(list)
        @test length(list) == 0

        for i in 6:10
            push!(list, i)
        end
        @test length(list) == 5
        @test collect(list) == [6, 7, 8, 9, 10]
    end

    @testset "Reuse of nodes" begin
        list = ReusableLinkedList{Int}(0, 3)
        for i in 1:3
            push!(list, i)
        end
        @test_throws ErrorException push!(list, 4)  # No more free nodes

        pop!(list)
        push!(list, 4)  # Should work now
        @test collect(list) == [1, 2, 4]
    end

    @testset "Vector-like operations" begin
        list = ReusableLinkedList{Int}(0, 5)
        vector = Int[]

        for i in 1:5
            push!(list, i)
            push!(vector, i)
            @test length(list) == length(vector)
            @test collect(list) == vector
        end
    end
end
