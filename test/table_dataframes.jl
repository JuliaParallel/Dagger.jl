import DataFrames
using Statistics

@testset "dtable-dataframes" begin
    @testset "select" begin
        s = 100_000
        nt = (a=collect(1:s) .% 3, b=rand(s))
        dt = DTable(nt, s ÷ 10)
        df = fetch(dt, DataFrames.DataFrame)

        t = (args...) -> begin
            dt_01 = Dagger.select(dt, args...)
            df_01 = DataFrames.select(df, args...)

            @test all(isapprox.(Tables.columns(df_01), Tables.columns(fetch(dt_01, DataFrame))))
        end

        t(:a)
        t(1)
        t(:b)
        t(2)
        t(:a, :b)
        t(1, 2)
        t(:b, :a)
        t(2, 1)
        t(:b, :a, AsTable([:a, :b]) => ByRow(sum))
        t(:b, :a, AsTable(:) => ByRow(sum))
        t([:a, :b] => ((x, y) -> x .+ y), :b, :a)
        t(:a => sum, :b, :a)
        t(:b => sum, :a => sum, :b, :a)
        t(names(dt) .=> sum, names(dt) .=> mean .=> "test" .* names(dt))
    end
end
