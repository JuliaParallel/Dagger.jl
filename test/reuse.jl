using Test
import Dagger: ReusableLinkedList, ReusableDict, ReusableCache
import Dagger: take_or_alloc!, maybe_take_or_alloc!, maybetake!, putback!

@testset "ReusableCache Tests" begin
    @testset "Construction and Basic Properties" begin
        # Test construction with different types and sizes
        cache_int = ReusableCache(Vector{Int}, 0, 3)  # Use 0 as null value for Int vectors
        cache_string = ReusableCache(Vector{String}, "", 5)
        cache_dict = ReusableCache(Dict{Int,String}, Dict{Int,String}(), 2)  # Use empty dict as null

        # Test sized cache construction
        cache_sized = ReusableCache(Vector{Float64}, 0.0, 4; sized=true)

        @test length(cache_int.cache) == 3
        @test length(cache_int.used) == 3
        @test all(cache_int.used .== false)
        @test cache_int.null == 0  # Should be 0, not nothing
        @test cache_int.sized == false

        @test length(cache_string.cache) == 5
        @test cache_string.null == ""
        @test cache_string.sized == false

        @test cache_sized.sized == true
        @test cache_sized.null == 0.0
    end

    @testset "maybetake! Basic Operations" begin
        cache = ReusableCache(Vector{Int}, 0, 3)  # Use 0 as null value

        # Test taking from fresh cache
        result1 = maybetake!(cache)
        @test result1 !== nothing
        idx1, vec1 = result1
        @test idx1 == 1
        @test isa(vec1, Vector{Int})
        @test cache.used[idx1] == true

        # Test taking second entry
        result2 = maybetake!(cache)
        @test result2 !== nothing
        idx2, vec2 = result2
        @test idx2 == 2
        @test cache.used[idx2] == true

        # Test taking third entry
        result3 = maybetake!(cache)
        @test result3 !== nothing
        idx3, vec3 = result3
        @test idx3 == 3
        @test cache.used[idx3] == true

        # Test that cache is exhausted
        result4 = maybetake!(cache)
        @test result4 === nothing

        # Test that all entries are marked as used
        @test all(cache.used)
    end

    @testset "putback! Operations" begin
        cache = ReusableCache(Vector{String}, "", 2)  # Empty string is correct for String vectors

        # Take both entries
        result1 = maybetake!(cache)
        result2 = maybetake!(cache)
        @test result1 !== nothing && result2 !== nothing
        idx1, _ = result1
        idx2, _ = result2

        # Verify cache is exhausted
        @test maybetake!(cache) === nothing

        # Put back first entry
        putback!(cache, idx1)
        @test cache.used[idx1] == false
        @test cache.used[idx2] == true

        # Should be able to take one entry again
        result3 = maybetake!(cache)
        @test result3 !== nothing
        idx3, _ = result3
        @test idx3 == idx1  # Should reuse the same slot

        # Put back both entries
        putback!(cache, idx2)
        putback!(cache, idx3)
        @test all(cache.used .== false)

        # Should be able to take both again
        @test maybetake!(cache) !== nothing
        @test maybetake!(cache) !== nothing
        @test maybetake!(cache) === nothing
    end

    @testset "Sized Cache Operations" begin
        cache = ReusableCache(Vector{Float64}, 0.0, 4; sized=true)

        # Test taking with specific length
        result1 = maybetake!(cache, 5)
        @test result1 !== nothing
        idx1, vec1 = result1
        @test length(vec1) == 5
        putback!(cache, idx1)

        # Test taking with different length - should allocate new slot
        result2 = maybetake!(cache, 3)
        @test result2 !== nothing
        idx2, vec2 = result2
        @test length(vec2) == 3
        @test idx2 != idx1  # Should use different slot
        putback!(cache, idx2)

        # Test taking with original length again - should reuse the same slot
        result3 = maybetake!(cache, 5)
        @test result3 !== nothing
        idx3, vec3 = result3
        @test idx3 == idx1  # Should reuse first slot (same length)
        @test length(vec3) == 5
        putback!(cache, idx3)

        # Test taking with second length again - should reuse that slot
        result4 = maybetake!(cache, 3)
        @test result4 !== nothing
        idx4, vec4 = result4
        @test idx4 == idx2  # Should reuse second slot (same length)
        @test length(vec4) == 3
        putback!(cache, idx4)

        # Allocate slots for different sizes - each will permanently occupy a slot
        lengths = [10, 15]  # Only allocate 2 more since we already used 2 slots
        results = []
        for len in lengths
            result = maybetake!(cache, len)
            @test result !== nothing
            idx, vec = result
            @test length(vec) == len
            push!(results, (idx, vec, len))
        end

        # Cache should now be full (4 slots all allocated to specific lengths: 5, 3, 10, 15)
        @test maybetake!(cache, 20) === nothing  # No more slots available

        # Put back the recent allocations
        for (idx, _, _) in results
            putback!(cache, idx)
        end

        # Test that requesting existing lengths still works (reuses allocated slots)
        result_reuse_10 = maybetake!(cache, 10)
        @test result_reuse_10 !== nothing
        idx_reuse_10, vec_reuse_10 = result_reuse_10
        @test length(vec_reuse_10) == 10
        # Should reuse one of the slots we allocated for length 10
        @test idx_reuse_10 in [r[1] for r in results if r[3] == 10]
        putback!(cache, idx_reuse_10)

        result_reuse_5 = maybetake!(cache, 5)
        @test result_reuse_5 !== nothing
        idx_reuse_5, vec_reuse_5 = result_reuse_5
        @test length(vec_reuse_5) == 5
        @test idx_reuse_5 == idx1  # Should reuse the original slot for length 5
        putback!(cache, idx_reuse_5)

        # Verify that we still can't allocate new lengths (cache full with permanent allocations)
        @test maybetake!(cache, 25) === nothing
        @test maybetake!(cache, 30) === nothing
    end

    @testset "take_or_alloc! Operations" begin
        cache = ReusableCache(Vector{Int}, 0, 2)  # Use 0 as null value for Int vectors
        call_count = 0

        # Function to test with
        test_function = function(vec)
            call_count += 1
            push!(vec, call_count)
            return length(vec)
        end

        # Test basic take_or_alloc!
        result1 = take_or_alloc!(test_function, cache)
        @test result1 == 1
        @test call_count == 1

        # Cache entry should be returned and cleared
        @test all(cache.used .== false)

        # Test second call - vector should be filled with null values
        result2 = take_or_alloc!(test_function, cache)
        # The vector will be filled with null (0) values from unset!, so after push it will have those + new value
        @test call_count == 2

        # Test with sized cache
        sized_cache = ReusableCache(Vector{Float64}, 0.0, 2; sized=true)
        sized_function = function(vec)
            fill!(vec, 3.14)
            return sum(vec)
        end

        result3 = take_or_alloc!(sized_function, sized_cache, 5)
        @test result3 ≈ 5 * 3.14

        # Test cache exhaustion with no_alloc=true
        cache_small = ReusableCache(Vector{String}, "", 1)

        # Fill the cache
        idx, _ = maybetake!(cache_small)

        # This should fail with no_alloc=true
        @test_throws ErrorException take_or_alloc!(identity, cache_small; no_alloc=true)

        # Put back and try again - should work
        putback!(cache_small, idx)
        result4 = take_or_alloc!(x -> length(x), cache_small; no_alloc=true)
        @test result4 == 0  # Vector should be empty (newly allocated)
    end

    @testset "maybe_take_or_alloc! Operations" begin
        cache = ReusableCache(Dict{String,Int}, Dict{String,Int}(), 2)  # Use empty dict as null

        test_function = function(dict)
            dict["test"] = 42
            return length(dict)
        end

        # Test with provided value (should use it directly)
        provided_dict = Dict("existing" => 1)
        result1 = maybe_take_or_alloc!(test_function, cache, provided_dict)
        @test result1 == 2  # Should have both "existing" and "test"
        @test provided_dict["test"] == 42  # Original dict should be modified

        # Test with nothing (should fall back to cache)
        result2 = maybe_take_or_alloc!(test_function, cache, nothing)
        @test result2 == 1  # New dict should only have "test"

        # Test with sized cache and provided value
        sized_cache = ReusableCache(Vector{Int}, 0, 2; sized=true)  # Use 0 as null value
        provided_vec = [1, 2, 3, 4, 5]  # Length 5

        sized_function = function(vec)
            vec[1] = 100
            return vec[1]
        end

        result3 = maybe_take_or_alloc!(sized_function, sized_cache, provided_vec, 5)
        @test result3 == 100
        @test provided_vec[1] == 100  # Original vector modified

        # Test fallback to cache with length
        result4 = maybe_take_or_alloc!(sized_function, sized_cache, nothing, 3)
        @test result4 == 100  # Should work with new vector of length 3
    end

    @testset "Different Element Types" begin
        # Test with various data types

        # String vectors
        string_cache = ReusableCache(Vector{String}, "", 2)
        string_result = take_or_alloc!(string_cache) do vec
            # Vector is initialized with null values ("")
            push!(vec, "hello", "world")
            join(vec, " ")
        end
        @test string_result == "hello world"

        # Test reuse - vector should be filled with null values
        string_result2 = take_or_alloc!(string_cache) do vec
            # Vector should be filled with "" (null value), we'll clear it first for clean output
            empty!(vec)
            push!(vec, "foo", "bar")
            join(vec, " ")
        end
        @test string_result2 == "foo bar"

        # Dictionaries with complex types
        dict_cache = ReusableCache(Dict{Symbol, Vector{Int}}, Dict{Symbol, Vector{Int}}(), 2)
        dict_result = take_or_alloc!(dict_cache) do dict
            dict[:numbers] = [1, 2, 3]
            dict[:more] = [4, 5]
            sum(length(v) for v in values(dict))
        end
        @test dict_result == 5

        # Sets
        set_cache = ReusableCache(Set{String}, Set{String}(), 3)
        set_result = take_or_alloc!(set_cache) do set
            push!(set, "a", "b", "c", "a")  # Duplicate should be ignored
            length(set)
        end
        @test set_result == 3

        # Custom struct (simple case)
        mutable struct TestStruct
            x::Int
            y::String
        end
        TestStruct() = TestStruct(0, "")
        function Dagger.unset!(t::TestStruct, tnull::TestStruct)
            t.x = tnull.x
            t.y = tnull.y
        end
        function Dagger.alloc!(::Type{TestStruct})
            return TestStruct()
        end

        struct_cache = ReusableCache(TestStruct, TestStruct(0, ""), 2)
        struct_result = take_or_alloc!(struct_cache) do obj
            obj.x = 42
            obj.y = "test"
            obj.x + length(obj.y)
        end
        @test struct_result == 46
    end

    @testset "Cache State Integrity" begin
        cache = ReusableCache(Vector{Int}, 0, 3)  # Use 0 as null value for Int vectors

        # Test that used array stays consistent
        @test length(cache.used) == length(cache.cache)

        # Take all entries and verify used array
        indices = []
        for i in 1:3
            result = maybetake!(cache)
            @test result !== nothing
            idx, _ = result
            push!(indices, idx)
            @test cache.used[idx] == true
        end

        # Verify all unique indices
        @test length(unique(indices)) == 3
        @test Set(indices) == Set(1:3)

        # Put back in random order
        for idx in shuffle(indices)
            putback!(cache, idx)
            @test cache.used[idx] == false
        end

        # Verify all entries are available again
        @test all(cache.used .== false)
        for i in 1:3
            @test maybetake!(cache) !== nothing
        end
        @test maybetake!(cache) === nothing
    end

    @testset "Cache Never Enters Broken State" begin
        cache = ReusableCache(Vector{Float64}, NaN, 2)  # NaN is appropriate for Float64 vectors

        # Test multiple putback of same valid index (this can happen in error handling)
        result = maybetake!(cache)
        @test result !== nothing
        idx, vec = result
        @test isa(vec, Vector{Float64})

        # Put back once
        putback!(cache, idx)
        @test cache.used[idx] == false

        # Put back again - should not break anything (idempotent)
        @test_nowarn putback!(cache, idx)
        @test cache.used[idx] == false

        # Should still be able to take
        result2 = maybetake!(cache)
        @test result2 !== nothing
        idx2, vec2 = result2
        @test isa(vec2, Vector{Float64})
        putback!(cache, idx2)

        # Test that cache works normally after edge case operations
        result3 = maybetake!(cache)
        @test result3 !== nothing
        idx3, vec3 = result3
        @test isa(vec3, Vector{Float64})

        # Modify vector and put back
        resize!(vec3, 3)
        fill!(vec3, 1.5)
        putback!(cache, idx3)

        # Take again and verify it was properly reset
        result4 = maybetake!(cache)
        @test result4 !== nothing
        idx4, vec4 = result4
        @test idx4 == idx3  # Should reuse same slot
        @test length(vec4) == 3
        @test all(isnan, vec4)  # Should be filled with NaN (null value)
        putback!(cache, idx4)
    end

    @testset "Cache Does Not Generate Invalid Values" begin
        # Test with many operations to ensure consistency
        cache = ReusableCache(Vector{Int}, -999, 5)  # Use -999 as null value for Int vectors

        # Test that freshly allocated vectors are empty (for non-sized cache)
        result = maybetake!(cache)
        @test result !== nothing
        idx, vec = result
        @test isa(vec, Vector{Int})
        @test isempty(vec)  # Should start empty for non-sized cache
        putback!(cache, idx)

        # Test basic put/take cycle with proper filling
        result = maybetake!(cache)
        @test result !== nothing
        idx, vec = result

        # User fills vector properly
        resize!(vec, 5)
        fill!(vec, 42)
        @test all(x -> x == 42, vec)

        # Put back - should be filled with null values
        putback!(cache, idx)

        # Take again - should have null values
        result2 = maybetake!(cache)
        @test result2 !== nothing
        idx2, vec2 = result2
        @test idx2 == idx  # Should reuse same slot
        @test length(vec2) == 5
        @test all(x -> x == -999, vec2)  # Should be filled with null values

        putback!(cache, idx2)

        # Test what happens with empty vectors
        result3 = maybetake!(cache)
        @test result3 !== nothing
        idx3, vec3 = result3

        # User empties the vector
        empty!(vec3)
        @test isempty(vec3)

        # Put back empty vector
        putback!(cache, idx3)

        # Take again - should still be empty (unset! on empty vector does nothing)
        result4 = maybetake!(cache)
        @test result4 !== nothing
        idx4, vec4 = result4
        @test idx4 == idx3  # Should reuse same slot
        @test isempty(vec4)  # Should still be empty

        putback!(cache, idx4)

        # Test cache state integrity after many operations
        for iteration in 1:50
            # Take random number of entries
            taken = []
            n_take = rand(1:3)  # Take 1-3 entries

            for _ in 1:n_take
                result = maybetake!(cache)
                if result !== nothing
                    idx, vec = result
                    @test isa(vec, Vector{Int})

                    # Use the vector in a controlled way
                    resize!(vec, 3)
                    fill!(vec, iteration)  # Properly fill all elements
                    @test all(x -> x == iteration, vec)

                    push!(taken, idx)
                end
            end

            # Put back all taken entries
            for idx in taken
                putback!(cache, idx)
            end
        end

        # Final verification - all vectors should be properly reset
        final_taken = []
        for _ in 1:5
            result = maybetake!(cache)
            if result !== nothing
                idx, vec = result
                @test isa(vec, Vector{Int})
                # Vector should contain only null values if not empty
                if !isempty(vec)
                    @test all(x -> x == -999, vec)
                end
                push!(final_taken, idx)
            end
        end

        @test maybetake!(cache) === nothing
        @test length(final_taken) == 5
        @test Set(final_taken) == Set(1:5)
    end

    @testset "Sized Cache Length Consistency" begin
        cache = ReusableCache(Vector{Bool}, false, 4; sized=true)

        # Take with different lengths and verify
        lengths_and_indices = []
        for len in [3, 7, 3, 5]  # Note: duplicate length 3
            result = maybetake!(cache, len)
            @test result !== nothing
            idx, vec = result
            @test length(vec) == len
            push!(lengths_and_indices, (len, idx))
        end

        # Cache should be full
        @test maybetake!(cache, 10) === nothing

        # Put back all entries
        for (_, idx) in lengths_and_indices
            putback!(cache, idx)
        end

        # Request length 3 again - should reuse existing entry
        result1 = maybetake!(cache, 3)
        @test result1 !== nothing
        idx1, vec1 = result1
        @test length(vec1) == 3

        # The index should be one of the indices that had length 3
        length_3_indices = [idx for (len, idx) in lengths_and_indices if len == 3]
        @test idx1 in length_3_indices

        putback!(cache, idx1)

        # Request different length - should use different slot or allocate new
        result2 = maybetake!(cache, 7)
        @test result2 !== nothing
        idx2, vec2 = result2
        @test length(vec2) == 7
    end
end

@testset "ReusableLinkedList Tests" begin

    @testset "Construction and Basic Properties" begin
        # Test construction with various types
        list_int = ReusableLinkedList{Int}(0, 5)
        list_str = ReusableLinkedList{String}("", 3)
        list_float = ReusableLinkedList{Float64}(0.0, 10)

        # Test initial state
        @test length(list_int) == 0
        @test length(list_str) == 0
        @test length(list_float) == 0

        # Test empty list doesn't contain any values
        @test !in(0, list_int)
        @test !in("test", list_str)
        @test !in(1.0, list_float)

        # Test iteration over empty list
        count = 0
        for item in list_int
            count += 1
        end
        @test count == 0
    end

    @testset "Invalid Operations on Empty Lists" begin
        list = ReusableLinkedList{Int}(0, 5)

        # Test invalid index access
        @test_throws BoundsError list[1]
        @test_throws BoundsError list[0]
        @test_throws BoundsError list[-1]
        @test_throws BoundsError list[10]

        # Test invalid setindex on empty list
        @test_throws BoundsError list[1] = 42

        # Test pop operations on empty list
        @test_throws ArgumentError pop!(list)
        @test_throws ArgumentError popfirst!(list)

        # Test deleteat! on empty list
        @test_throws BoundsError deleteat!(list, 1)

        # Test findfirst on empty list (should return nothing)
        @test findfirst(x -> x == 5, list) === nothing

        # Test empty! on already empty list (should not error)
        @test_nowarn empty!(list)
        @test length(list) == 0

        # Test resize! to 0 on empty list
        @test_nowarn resize!(list, 0)
        @test length(list) == 0
    end

    @testset "Push and Pop Operations" begin
        list = ReusableLinkedList{Int}(0, 5)

        # Test push! operations
        push!(list, 1)
        @test length(list) == 1
        @test list[1] == 1

        push!(list, 2)
        push!(list, 3)
        @test length(list) == 3
        @test list[1] == 1
        @test list[2] == 2
        @test list[3] == 3

        # Test pushfirst! operations
        pushfirst!(list, 0)
        @test length(list) == 4
        @test list[1] == 0
        @test list[2] == 1
        @test list[3] == 2
        @test list[4] == 3

        # Test capacity limit
        push!(list, 4)  # Should reach capacity
        @test length(list) == 5
        @test_throws ArgumentError push!(list, 5)  # Should exceed capacity
        @test_throws ArgumentError pushfirst!(list, -1)  # Should exceed capacity

        # Test pop! operations
        val = pop!(list)
        @test val == 4
        @test length(list) == 4

        val = popfirst!(list)
        @test val == 0
        @test length(list) == 3
        @test list[1] == 1
        @test list[2] == 2
        @test list[3] == 3
    end

    @testset "Indexing Operations" begin
        list = ReusableLinkedList{String}("", 4)
        push!(list, "a")
        push!(list, "b")
        push!(list, "c")

        # Test getindex
        @test list[1] == "a"
        @test list[2] == "b"
        @test list[3] == "c"

        # Test invalid getindex
        @test_throws BoundsError list[0]
        @test_throws BoundsError list[4]
        @test_throws BoundsError list[-1]

        # Test setindex!
        list[2] = "modified"
        @test list[2] == "modified"
        @test list[1] == "a"  # Other elements unchanged
        @test list[3] == "c"

        # Test invalid setindex!
        @test_throws BoundsError list[0] = "invalid"
        @test_throws BoundsError list[4] = "invalid"
        @test_throws BoundsError list[-1] = "invalid"
    end

    @testset "Search and Membership" begin
        list = ReusableLinkedList{Int}(0, 6)
        for i in [10, 20, 30, 20, 40]
            push!(list, i)
        end

        # Test in operation
        @test in(10, list)
        @test in(20, list)
        @test in(30, list)
        @test in(40, list)
        @test !in(50, list)
        @test !in(0, list)  # null element not in list

        # Test findfirst
        @test findfirst(x -> x == 10, list) == 1
        @test findfirst(x -> x == 20, list) == 2  # First occurrence
        @test findfirst(x -> x == 40, list) == 5
        @test findfirst(x -> x == 50, list) === nothing
        @test findfirst(x -> x > 25, list) == 3  # First element > 25
    end

    @testset "Iteration" begin
        list = ReusableLinkedList{Float64}(0.0, 4)
        values = [1.1, 2.2, 3.3, 4.4]
        for v in values
            push!(list, v)
        end

        # Test basic iteration
        collected = Float64[]
        for item in list
            push!(collected, item)
        end
        @test collected == values

        # Test iteration with enumerate
        for (i, item) in enumerate(list)
            @test item == values[i]
        end

        # Test collect
        @test collect(list) == values
    end

    @testset "Bulk Operations" begin
        list = ReusableLinkedList{Int}(0, 10)

        # Test fill!
        resize!(list, 5)
        fill!(list, 42)
        @test length(list) == 5
        for i in 1:5
            @test list[i] == 42
        end

        # Test empty! and refill
        empty!(list)
        @test length(list) == 0

        # Test resize! with various sizes
        resize!(list, 3)
        @test length(list) == 3

        # Fill with test data
        for i in 1:3
            list[i] = i * 10
        end

        # Test resize! to larger size
        resize!(list, 6)
        @test length(list) == 6
        @test list[1] == 10
        @test list[2] == 20
        @test list[3] == 30
        # New elements should be initialized to null value

        # Test resize! to smaller size
        resize!(list, 2)
        @test length(list) == 2
        @test list[1] == 10
        @test list[2] == 20

        # Test resize! beyond capacity
        @test_throws ArgumentError resize!(list, 11)
    end

    @testset "Deletion Operations" begin
        list = ReusableLinkedList{Int}(0, 8)
        for i in 1:5
            push!(list, i * 10)
        end

        # Test deleteat! from middle
        deleteat!(list, 3)  # Remove 30
        @test length(list) == 4
        @test list[1] == 10
        @test list[2] == 20
        @test list[3] == 40  # 40 moved to position 3
        @test list[4] == 50

        # Test deleteat! from beginning
        deleteat!(list, 1)
        @test length(list) == 3
        @test list[1] == 20
        @test list[2] == 40
        @test list[3] == 50

        # Test deleteat! from end
        deleteat!(list, 3)
        @test length(list) == 2
        @test list[1] == 20
        @test list[2] == 40

        # Test invalid deleteat!
        @test_throws BoundsError deleteat!(list, 0)
        @test_throws BoundsError deleteat!(list, 3)
        @test_throws BoundsError deleteat!(list, -1)
    end

    @testset "Map and Copy Operations" begin
        list = ReusableLinkedList{Int}(0, 6)
        for i in 1:4
            push!(list, i)
        end

        # Test map!
        map!(x -> x * 2, list, list)
        @test list[1] == 2
        @test list[2] == 4
        @test list[3] == 6
        @test list[4] == 8

        # Test copyto! with array
        src = [100, 200, 300]
        copyto!(list, src)
        @test length(list) == 4
        @test list[1] == 100
        @test list[2] == 200
        @test list[3] == 300
        @test list[4] == 8

        # Test copyto! with another list
        list2 = ReusableLinkedList{Int}(0, 6)
        push!(list2, 999)
        push!(list2, 888)

        copyto!(list, list2)
        @test length(list) == 4
        @test list[1] == 999
        @test list[2] == 888
        @test list[3] == 300
        @test list[4] == 8

        # Test copyto! exceeding capacity
        large_src = collect(1:10)
        list_small = ReusableLinkedList{Int}(0, 5)
        @test_throws BoundsError copyto!(list_small, large_src)
    end
end

@testset "ReusableDict Tests" begin

    @testset "Construction and Basic Properties" begin
        # Test construction with various types
        dict_int = ReusableDict{String, Int}("", 0, 5)
        dict_str = ReusableDict{Int, String}(0, "", 3)
        dict_float = ReusableDict{String, Float64}("", 0.0, 10)

        # Test initial state
        @test length(dict_int) == 0
        @test length(dict_str) == 0
        @test length(dict_float) == 0

        # Test empty dict doesn't contain any keys
        @test !haskey(dict_int, "test")
        @test !haskey(dict_str, 1)
        @test !haskey(dict_float, "pi")

        # Test iteration over empty dict
        count = 0
        for (k, v) in dict_int
            count += 1
        end
        @test count == 0

        # Test keys and values on empty dict
        @test length(keys(dict_int)) == 0
        @test length(values(dict_int)) == 0
    end

    @testset "Invalid Operations on Empty Dicts" begin
        dict = ReusableDict{String, Int}("", 0, 5)

        # Test invalid key access
        @test_throws KeyError dict["nonexistent"]

        # Test delete! on non-existent key
        @test_throws KeyError delete!(dict, "nonexistent")

        # Test empty! on already empty dict (should not error)
        @test_nowarn empty!(dict)
        @test length(dict) == 0
    end

    @testset "Basic Dictionary Operations" begin
        dict = ReusableDict{String, Int}("", 0, 4)

        # Test setindex! and getindex
        dict["a"] = 1
        @test dict["a"] == 1
        @test length(dict) == 1
        @test haskey(dict, "a")

        dict["b"] = 2
        dict["c"] = 3
        @test dict["b"] == 2
        @test dict["c"] == 3
        @test length(dict) == 3

        # Test updating existing key
        dict["a"] = 10
        @test dict["a"] == 10
        @test length(dict) == 3  # Length shouldn't change

        # Test capacity limit
        dict["d"] = 4
        @test length(dict) == 4
        @test_throws ArgumentError dict["e"] = 5  # Should exceed capacity

        # Test getindex with non-existent key
        @test_throws KeyError dict["nonexistent"]
    end

    @testset "Key Membership and Search" begin
        dict = ReusableDict{Int, String}(0, "", 5)
        dict[10] = "ten"
        dict[20] = "twenty"
        dict[30] = "thirty"

        # Test haskey
        @test haskey(dict, 10)
        @test haskey(dict, 20)
        @test haskey(dict, 30)
        @test !haskey(dict, 40)
        @test !haskey(dict, 0)  # null key not in dict

        # Test that null values don't interfere
        dict[40] = ""  # Setting to null value should still work
        @test haskey(dict, 40)
        @test dict[40] == ""
    end

    @testset "Keys and Values" begin
        dict = ReusableDict{String, Float64}("", 0.0, 6)
        test_data = Dict("pi" => 3.14, "e" => 2.71, "phi" => 1.61)

        for (k, v) in test_data
            dict[k] = v
        end

        # Test keys()
        dict_keys = collect(keys(dict))
        @test length(dict_keys) == 3
        for k in ["pi", "e", "phi"]
            @test k in dict_keys
        end

        # Test values()
        dict_values = collect(values(dict))
        @test length(dict_values) == 3
        for v in [3.14, 2.71, 1.61]
            @test v in dict_values
        end

        # Test keys and values are consistent
        for k in keys(dict)
            @test haskey(test_data, k)
            @test dict[k] == test_data[k]
        end
    end

    @testset "Iteration" begin
        dict = ReusableDict{Int, String}(0, "", 4)
        test_pairs = [(1, "one"), (2, "two"), (3, "three")]

        for (k, v) in test_pairs
            dict[k] = v
        end

        # Test iteration over key-value pairs
        collected_pairs = []
        for (k, v) in dict
            push!(collected_pairs, (k, v))
        end
        @test length(collected_pairs) == 3

        for pair in test_pairs
            @test pair in collected_pairs
        end

        # Test that iteration order is consistent with keys/values
        keys_iter = collect(keys(dict))
        values_iter = collect(values(dict))
        pairs_iter = collect(dict)

        for i in 1:length(pairs_iter)
            k, v = pairs_iter[i]
            @test k == keys_iter[i]
            @test v == values_iter[i]
            @test dict[k] == v
        end
    end

    @testset "Deletion Operations" begin
        dict = ReusableDict{String, Int}("", 0, 6)

        # Add test data
        for (k, v) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
            dict[k] = v
        end
        @test length(dict) == 4

        # Test delete! existing key
        delete!(dict, "b")
        @test length(dict) == 3
        @test !haskey(dict, "b")
        @test haskey(dict, "a")
        @test haskey(dict, "c")
        @test haskey(dict, "d")

        # Test delete! non-existent key
        @test_throws KeyError delete!(dict, "nonexistent")
        @test_throws KeyError delete!(dict, "b")  # Already deleted

        # Test that we can add new keys after deletion
        dict["e"] = 5
        @test length(dict) == 4
        @test dict["e"] == 5

        # Test deleting all remaining keys
        for k in ["a", "c", "d", "e"]
            delete!(dict, k)
        end
        @test length(dict) == 0
    end

    @testset "Empty! Operation" begin
        dict = ReusableDict{Int, String}(0, "", 5)

        # Add some data
        for i in 1:4
            dict[i] = "value$i"
        end
        @test length(dict) == 4

        # Test empty!
        empty!(dict)
        @test length(dict) == 0

        # Test that all keys are gone
        for i in 1:4
            @test !haskey(dict, i)
        end

        # Test that we can add new data after empty!
        dict[100] = "new value"
        @test length(dict) == 1
        @test dict[100] == "new value"

        # Test empty! on already empty dict
        empty!(dict)
        @test_nowarn empty!(dict)  # Should not error
        @test length(dict) == 0
    end

    @testset "Capacity and Edge Cases" begin
        dict = ReusableDict{Int, Int}(0, 0, 3)

        # Fill to capacity
        dict[1] = 10
        dict[2] = 20
        dict[3] = 30
        @test length(dict) == 3

        # Test exceeding capacity
        @test_throws ArgumentError dict[4] = 40

        # Test that updating existing keys doesn't exceed capacity
        @test_nowarn dict[1] = 100
        @test dict[1] == 100
        @test length(dict) == 3

        # Test with null values and keys
        dict_nulls = ReusableDict{Int, String}(-1, "NULL", 3)
        dict_nulls[0] = "zero"  # Non-null key, non-null value
        dict_nulls[1] = "NULL"  # Non-null key, null value
        @test length(dict_nulls) == 2
        @test dict_nulls[0] == "zero"
        @test dict_nulls[1] == "NULL"
        @test haskey(dict_nulls, 0)
        @test haskey(dict_nulls, 1)
        @test !haskey(dict_nulls, -1)  # null key should not be present
    end

    @testset "Type Consistency" begin
        # Test with different key-value type combinations
        dict_si = ReusableDict{String, Int}("", 0, 3)
        dict_is = ReusableDict{Int, String}(0, "", 3)
        dict_ff = ReusableDict{Float64, Float64}(0.0, 0.0, 3)

        # Test type enforcement
        dict_si["test"] = 42
        @test dict_si["test"] == 42

        dict_is[42] = "test"
        @test dict_is[42] == "test"

        dict_ff[3.14] = 2.71
        @test dict_ff[3.14] == 2.71

        # Ensure operations maintain type consistency
        @test typeof(collect(keys(dict_si))[1]) == String
        @test typeof(collect(values(dict_si))[1]) == Int
        @test typeof(collect(keys(dict_is))[1]) == Int
        @test typeof(collect(values(dict_is))[1]) == String
    end
end

@testset "Cross-Structure Interaction Tests" begin
    @testset "Using Structures Together" begin
        # Test using both structures in combination
        list = ReusableLinkedList{String}("", 5)
        dict = ReusableDict{String, Int}("", 0, 5)

        # Add data to list
        for word in ["apple", "banana", "cherry"]
            push!(list, word)
        end

        # Create dictionary with list contents as keys
        for (i, word) in enumerate(list)
            dict[word] = i
        end

        @test length(dict) == 3
        @test dict["apple"] == 1
        @test dict["banana"] == 2
        @test dict["cherry"] == 3

        # Test that modifications to one don't affect the other
        list[1] = "apricot"
        @test dict["apple"] == 1  # Dict unchanged
        @test !haskey(dict, "apricot")  # New value not in dict
    end
end