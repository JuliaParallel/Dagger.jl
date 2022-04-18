using DataFrames
using Statistics

@testset "dtable-dataframes" begin
    @testset "select" begin
        nt = (a=collect(1:100).%3, b=rand(100))
        dt = DTable(nt, 10)
        df = fetch(dt, DataFrame)

        dt_01 = Dagger.select(dt, :a)
        df_01 = DataFrames.select(df, :a)
        @test df_01 == fetch(dt_01, DataFrame)

        dt_02 = Dagger.select(dt, :b, :a)
        df_02 = DataFrames.select(df, :b, :a)
        @test df_02 == fetch(dt_02, DataFrame)
    end
end
