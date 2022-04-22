using DataFrames
using Statistics

@testset "dtable-dataframes" begin
    @testset "select" begin
        nt = (a=collect(1:100).%3, b=rand(100))
        dt = DTable(nt, 10)
        df = fetch(dt, DataFrame)

        t = (args...) -> begin
            dt_01 = Dagger.select(dt, args...)
            df_01 = DataFrames.select(df, args...)
            @test df_01 == fetch(dt_01, DataFrame)
        end

        t(:a); t(1)
        t(:b); t(2)
        t(:a, :b); t(1, 2)
        t(:b, :a); t(2, 1)
        t(:b, :a, AsTable([:a,:b]) => ByRow(sum))
        t(:b, :a, AsTable(:) => ByRow(sum))
    end
end
