# Distributed Arrays

The `DArray`, or "distributed array", is an abstraction layer on top of Dagger
that allows loading array-like structures into a distributed environment. The
`DArray` partitions a larger array into smaller "blocks" or "chunks", and those
blocks may be located on any worker in the cluster. The `DArray` uses a
Parallel Global Address Space (aka "PGAS") model for storing partitions, which
means that a `DArray` instance contains a reference to every partition in the
greater array; this provides great flexibility in allowing Dagger to choose the
most efficient way to distribute the array's blocks and operate on them in a
heterogeneous manner.

Aside: an alternative model, here termed the "MPI" model, is not yet supported,
but would allow storing only a single partition of the array on each MPI rank
in an MPI cluster. `DArray` support for this model is planned in the near
future.

This should not be confused with the [DistributedArrays.jl](https://github.com/JuliaParallel/DistributedArrays.jl) package.

## Creating `DArrays`

A `DArray` can be created in two ways: through an API similar to the usual
`rand`, `ones`, etc. calls, or by distributing an existing array with
`distribute`. It's generally not recommended to manually construct a `DArray`
object unless you're developing the `DArray` itself.

### Allocating new arrays

As an example, one can allocate a random `DArray` by calling `rand` with a
`Blocks` object as the first argument - `Blocks` specifies the size of
partitions to be constructed, and must be the same number of dimensions as the
array being allocated.

```julia
# Add some Julia workers
julia> using Distributed; addprocs(6)
6-element Vector{Int64}:
 2
 3
 4
 5
 6
 7

julia> @everywhere using Dagger

julia> DX = rand(Blocks(50, 50), 100, 100)
Dagger.DArray{Any, 2, typeof(cat)}(100, 100)
```

The `rand(Blocks(50, 50), 100, 100)` call specifies that a `DArray` matrix
should be allocated which is in total 100 x 100, split into 4 blocks of size 50
x 50, and initialized with random `Float64`s. Many other functions, like
`randn`, `ones`, and `zeros` can be called in this same way.

Note that the `DArray` is an asynchronous object (i.e. operations on it may
execute in the background), so to force it to be materialized, `fetch` may need
to be called:

```julia
julia> fetch(DX)
Dagger.DArray{Any, 2, typeof(cat)}(100, 100)
```

This doesn't change the type or values of the `DArray`, but it does make sure
that any pending operations have completed.

To convert a `DArray` back into an `Array`, `collect` can be used to gather the
data from all the Julia workers that they're on and combine them into a single
`Array` on the worker calling `collect`:

```julia
julia> collect(DX)
100×100 Matrix{Float64}:
 0.610404   0.0475367  0.809016   0.311305   0.0306211   0.689645   …  0.220267   0.678548   0.892062    0.0559988
 0.680815   0.788349   0.758755   0.0594709  0.640167    0.652266      0.331429   0.798848   0.732432    0.579534
 0.306898   0.0805607  0.498372   0.887971   0.244104    0.148825      0.340429   0.029274   0.140624    0.292354
 0.0537622  0.844509   0.509145   0.561629   0.566584    0.498554      0.427503   0.835242   0.699405    0.0705192
 0.587364   0.59933    0.0624318  0.3795     0.430398    0.0853735     0.379947   0.677105   0.0305861   0.748001
 0.14129    0.635562   0.218739   0.0629501  0.373841    0.439933   …  0.308294   0.0966736  0.783333    0.00763648
 0.14539    0.331767   0.912498   0.0649541  0.527064    0.249595      0.826705   0.826868   0.41398     0.80321
 0.13926    0.353158   0.330615   0.438247   0.284794    0.238837      0.791249   0.415801   0.729545    0.88308
 0.769242   0.136001   0.950214   0.171962   0.183646    0.78294       0.570442   0.321894   0.293101    0.911913
 0.786168   0.513057   0.781712   0.0191752  0.512821    0.621239      0.50503    0.0472064  0.0368674   0.75981
 0.493378   0.129937   0.758052   0.169508   0.0564534   0.846092   …  0.873186   0.396222   0.284       0.0242124
 0.12689    0.194842   0.263186   0.213071   0.535613    0.246888      0.579931   0.699231   0.441449    0.882772
 0.916144   0.21305    0.629293   0.329303   0.299889    0.127453      0.644012   0.311241   0.713782    0.0554386
 ⋮                                                       ⋮          ⋱
 0.430369   0.597251   0.552528   0.795223   0.46431     0.777119      0.189266   0.499178   0.715808    0.797629
 0.235668   0.902973   0.786537   0.951402   0.768312    0.633666      0.724196   0.866373   0.0679498   0.255039
 0.605097   0.301349   0.758283   0.681568   0.677913    0.51507    …  0.654614   0.37841    0.86399     0.583924
 0.824216   0.62188    0.369671   0.725758   0.735141    0.183666      0.0401394  0.522191   0.849429    0.839651
 0.578047   0.775035   0.704695   0.203515   0.00267523  0.869083      0.0975535  0.824887   0.00787017  0.920944
 0.805897   0.0275489  0.175715   0.135956   0.389958    0.856349      0.974141   0.586308   0.59695     0.906727
 0.212875   0.509612   0.85531    0.266659   0.0695836   0.0551129     0.788085   0.401581   0.948216    0.00242077
 0.512997   0.134833   0.895968   0.996953   0.422192    0.991526   …  0.838781   0.141053   0.747722    0.84489
 0.283221   0.995152   0.61636    0.75955    0.072718    0.691665      0.151339   0.295759   0.795476    0.203072
 0.0946639  0.496832   0.551496   0.848571   0.151074    0.625696      0.673817   0.273958   0.177998    0.563221
 0.0900806  0.127274   0.394169   0.140403   0.232985    0.460306      0.536441   0.200297   0.970311    0.0292218
 0.0698985  0.463532   0.934776   0.448393   0.606287    0.552196      0.883694   0.212222   0.888415    0.941097
```

### Distributing existing arrays

Now let's look at constructing a `DArray` from an existing array object; we can
do this by calling `distribute`:

```julia
julia> Z = zeros(100, 500);

julia> Dzeros = distribute(Z, Blocks(10, 50))
Dagger.DArray{Any, 2, typeof(cat)}(100, 500)
```

This will distribute the array partitions (in chunks of 10 x 50 matrices)
across the workers in the Julia cluster in a relatively even distribution;
future operations on a `DArray` may produce a different distribution from the
one chosen by `distribute`.

## Broadcasting

As the `DArray` is a subtype of `AbstractArray` and generally satisfies Julia's
array interface, a variety of common operations (such as broadcast) work as
expected:

```julia
julia> DX = rand(Blocks(50,50), 100, 100)
Dagger.DArray{Float64, 2, Blocks{2}, typeof(cat)}(100, 100)

julia> DY = DX .+ DX
Dagger.DArray{Float64, 2, Blocks{2}, typeof(cat)}(100, 100)

julia> DZ = DY .* 3
Dagger.DArray{Float64, 2, Blocks{2}, typeof(cat)}(100, 100)
```

Now, `DZ` will contain the result of computing `(DX .+ DX) .* 3`. Note that
`DArray` objects are immutable, and operations on them are thus functional
transformations of their input `DArray`.

!!! note
    Support for mutation of `DArray`s is planned for a future release

```
julia> Dagger.chunks(DZ)
2×2 Matrix{Any}:
 EagerThunk (finished)  EagerThunk (finished)
 EagerThunk (finished)  EagerThunk (finished)

julia> Dagger.chunks(fetch(DZ))
2×2 Matrix{Union{Thunk, Dagger.Chunk}}:
 Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(4, 8, 0x0000000000004e20), ThreadProc(4, 1), AnyScope(), true)  …  Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(2, 5, 0x0000000000004e20), ThreadProc(2, 1), AnyScope(), true)
 Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(5, 5, 0x0000000000004e20), ThreadProc(5, 1), AnyScope(), true)     Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(3, 3, 0x0000000000004e20), ThreadProc(3, 1), AnyScope(), true)
```

Here we can see the `DArray`'s internal representation of the partitions, which
are stored as either `EagerThunk` objects (representing an ongoing or completed
computation) or `Chunk` objects (which reference data which exist locally or on
other Julia workers). Of course, one doesn't typically need to worry about
these internal details unless implementing low-level operations on `DArray`s.

Finally, it's easy to see the results of this combination of broadcast
operations; just use `collect` to get an `Array`:

```
julia> collect(DZ)
100×100 Matrix{Float64}:
 5.72754    1.23614   4.67045     4.89095   3.40126    …  5.07663     1.60482    5.04386    1.44755   2.5682
 0.189402   3.64462   5.92218     3.94603   2.32192       1.47115     4.6364     0.778867   3.13838   4.87871
 3.3492     3.96929   3.46377     1.29776   3.59547       4.82616     1.1512     3.02528    3.05538   0.139763
 5.0981     5.72564   5.1128      0.954708  2.04515       2.50365     5.97576    5.17683    4.79587   1.80113
 1.0737     5.25768   4.25363     0.943006  4.25783       4.1801      3.14444    3.07428    4.41075   2.90252
 5.48746    5.17286   3.99259     0.939678  3.76034    …  0.00763076  2.98176    1.83674    1.61791   3.33216
 1.05088    4.98731   1.24925     3.57909   2.53366       5.96733     2.35186    5.75815    3.32867   1.15317
 0.0335647  3.52524   0.159895    5.49908   1.33206       3.51113     0.0753356  1.5557     0.884252  1.45085
 5.27506    2.00472   0.00636555  0.461574  5.16735       2.74457     1.14679    2.39407    0.151713  0.85013
 4.43607    4.50304   4.73833     1.92498   1.64338       4.34602     4.62612    3.28248    1.32726   5.50207
 5.22308    2.53069   1.27758     2.62013   3.73961    …  5.91626     2.54943    5.41472    1.67197   4.09026
 1.09684    2.53189   4.23236     0.14055   0.889771      2.20834     2.31341    5.23121    1.74341   4.00588
 2.55253    4.1789    3.50287     4.96437   1.26724       3.04302     3.74262    5.46611    1.39375   4.13167
 3.03291    4.43932   2.85678     1.59531   0.892166      0.414873    0.643423   4.425      5.48145   5.93383
 0.726568   0.516686  3.00791     3.76354   3.32603       2.19812     2.15836    3.85669    3.67233   2.1261
 2.22763    1.36281   4.41129     5.29229   1.10093    …  0.45575     4.38389    0.0526105  2.14792   2.26734
 2.58065    1.99564   4.82657     0.485823  5.24881       2.16097     3.59942    2.25021    3.96498   0.906153
 0.546354   0.982523  1.94377     2.43136   2.77469       4.43507     5.98402    0.692576   1.53298   1.20621
 4.71374    4.99402   1.5876      1.81629   2.56269       1.56588     5.42296    0.160867   4.17705   1.13915
 2.97733    2.4476    3.82752     1.3491    3.5684        1.23393     1.86595    3.97154    4.6419    4.8964
 ⋮                                                     ⋱  ⋮
 3.49162    2.46081   1.21659     2.96078   4.58102       5.97679     3.34463    0.202255   2.85433   0.0786219
 0.894714   2.87079   5.09409     2.2922    3.18928       1.5886      0.163886   5.99251    0.697163  5.75684
 2.98867    2.2115    5.07771     0.124194  3.88948       3.61176     0.0732554  4.11606    0.424547  0.621287
 5.95438    3.45065   0.194537    3.57519   1.2266        2.93837     1.02609    5.84021    5.498     3.53337
 2.234      0.275185  0.648536    0.952341  4.41942    …  4.78238     2.24479    3.31705    5.76518   0.621195
 5.54212    2.24089   5.81702     1.96178   4.99409       0.30557     3.55499    0.851678   1.80504   5.81679
 5.79409    4.86848   3.10078     4.22252   4.488         3.03427     2.32752    3.54999    0.967972  4.0385
 3.06557    5.4993    2.44263     1.82296   0.166883      0.763588    1.59113    4.33305    2.8359    5.56667
 3.86797    3.73251   3.14999     4.11437   0.454938      0.166886    0.303827   4.7934     3.37593   2.29402
 0.762158   4.3716    0.897798    4.60541   2.96872    …  1.60095     0.480542   1.41945    1.33071   0.308611
 1.20503    5.66645   4.03237     3.90194   1.55996       3.58442     4.6735     5.52211    5.46891   2.43612
 5.51133    1.13591   3.26696     4.24821   4.60696       3.73251     3.25989    4.735      5.61674   4.32185
 2.46529    0.444928  3.85984     5.49469   1.13501       1.36861     5.34651    0.398515   0.239671  5.36412
 2.62837    3.99017   4.52569     3.54811   3.35515       4.13514     1.22304    1.01833    3.42534   3.58399
 4.88289    5.09945   0.267154    3.38482   4.53408    …  3.71752     5.22216    1.39987    1.38622   5.47351
 0.1046     3.65967   1.62098     5.33185   0.0822769     3.30334     5.90173    4.06603    5.00789   4.40601
 1.9622     0.755491  2.12264     1.67299   2.34482       4.50632     3.84387    3.22232    5.23164   2.97735
 4.37208    5.15253   0.346373    2.98573   5.48589       0.336134    2.25751    2.39057    1.97975   3.24243
 3.83293    1.69017   3.00189     1.80388   3.43671       5.94085     1.27609    3.98737    0.334963  5.84865
```

A variety of other operations exist on the `DArray`, and it should generally
behavior otherwise similar to any other `AbstractArray` type. If you find that
it's missing an operation that you need, please file an issue!

### Known Supported Operations

This list is not exhaustive, but documents operations which are known to work well with the `DArray`:

From `Base`:
- Broadcasting
- `map`/`reduce`/`mapreduce`
- `sum`/`prod`
- `minimum`/`maximum`/`extrema`

From `Statistics`:
- `mean`
- `var`
- `std`
