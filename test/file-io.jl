using Serialization

@testset "File IO" begin
    @testset "File" begin
        data = [1,2,3]
        data_path = joinpath(tempdir(), "jl_" * join(rand('a':'z', 8)) * ".jls")
        atexit() do
            @assert isfile(data_path)
            rm(data_path)
        end
        serialize(data_path, data)

        data_c = Dagger.File(data_path)::Dagger.File
        @test fetch(data_c) == data
        @test fetch(Dagger.@spawn identity(data_c)) == data

        @test isfile(data_path)
    end
    @testset "tofile" begin
        data = [4,5,6]
        data_path = joinpath(tempdir(), "jl_" * join(rand('a':'z', 8)) * ".jls")
        atexit() do
            @assert isfile(data_path)
            rm(data_path)
        end

        data_c = Dagger.tofile(data, data_path)::Dagger.File
        @test fetch(data_c) == data
        @test fetch(Dagger.@spawn identity(data_c)) == data

        @test isfile(data_path)
    end
end
