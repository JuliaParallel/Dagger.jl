using Serialization

@testset "File IO" begin
    @testset "File" begin
        data = [1,2,3]
        # test both absolute and relative paths
        dir_abs = tempdir()
        dir_rel = splitpath(mktempdir(pwd()))[end]
        for dir in [dir_abs, dir_rel]
            # Testsets restore RNG state, so random names can collide between
            # File and tofile. Leave cleanup to the retention assertion below.
            data_path = tempname(dir; cleanup=false)
            atexit() do
                @assert isfile(data_path) "Retained File disappeared: $(abspath(data_path))"
                rm(data_path)
            end
            serialize(data_path, data)

            data_c = Dagger.File(data_path)::Dagger.File
            @test fetch(data_c) == data
            @test fetch(Dagger.@spawn identity(data_c)) == data

            @test isfile(data_path)
        end
    end
    @testset "tofile" begin
        data = [4,5,6]
        # test both absolute and relative paths
        dir_abs = tempdir()
        dir_rel = splitpath(mktempdir(pwd()))[end]
        for dir in [dir_abs, dir_rel]
            data_path = tempname(dir; cleanup=false)
            atexit() do
                @assert isfile(data_path) "Retained tofile disappeared: $(abspath(data_path))"
                rm(data_path)
            end

            data_c = Dagger.tofile(data, data_path)::Dagger.File
            @test fetch(data_c) == data
            @test fetch(Dagger.@spawn identity(data_c)) == data

            @test isfile(data_path)
        end
    end
end
