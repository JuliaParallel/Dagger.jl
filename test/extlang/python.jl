using PythonCall
using CondaPkg

CondaPkg.add("numpy")

np = pyimport("numpy")

# Restart scheduler to see new methods
Dagger.cancel!(;halt_sch=true)

@testset "spawn" begin
    a = np.array([1, 2, 3])

    t = Dagger.@spawn np.sum(a)
    result = fetch(t)
    @test result isa Py
    @test pyconvert(Int, result) == 6

    b = np.array([4, 5, 6])

    t = Dagger.@spawn np.add(a, b)
    result = fetch(t)
    @test result isa Py
    @test pyconvert(Array, result) == [5, 7, 9]

    t2 = Dagger.@spawn np.add(t, b)
    result = fetch(t2)
    @test result isa Py
    @test pyconvert(Array, result) == [9, 12, 15]
end
