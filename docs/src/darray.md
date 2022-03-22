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
`distribute`. Additionally, most operations on `DArray`s also return `DArray`s
or an equivalent object which represents the operation being performed. It's
generally not recommended to manually construct a `DArray` object unless you're
developing the `DArray` itself.

### Allocating new arrays

As an example, one can allocate a random `DArray` by calling `rand` with a
`Blocks` object as the first argument - `Blocks` specifies the size of
partitions to be constructed. Note that the `DArray` is a lazy asynchronous
object (i.e. operations on it may execute in the background), so to force it to
be materialized, `fetch` may need to be called:

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
Dagger.AllocateArray{Float64, 2}(100, 100)

julia> fetch(DX)
Dagger.DArray{Any, 2, typeof(cat)}(100, 100)
```

The `rand(Blocks(50, 50), 100, 100)` call specifies that a `DArray` matrix
should be allocated which is in total 100 x 100, split into 4 blocks of size 50
x 50, and initialized with random `Float64`s. Many other functions, like
`randn`, `ones`, and `zeros` can be called in this same way.

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
do this by calling `Distribute`:

```julia
julia> Z = zeros(100, 500);

julia> Dzeros = Distribute(Blocks(10, 50), Z)
Distribute{Float64, 2}(100, 500)

julia> fetch(Dzeros)
Dagger.DArray{Any, 2, typeof(cat)}(100, 500)
```

If we wanted to skip having to call `fetch`, we could just call `distribute`,
which blocks until distributing the array is completed:

```julia
julia> Dzeros = distribute(Z, Blocks(10, 50))
Dagger.DArray{Any, 2, typeof(cat)}(100, 500)
```

## Broadcasting

As the `DArray` is a subtype of `AbstractArray` and generally satisfies Julia's
array interface, a variety of common operations (such as broadcast) work as
expected:

```julia
julia> DX = rand(Blocks(50,50), 100, 100)
Dagger.AllocateArray{Float64, 2}(100, 100)

julia> DY = DX .+ DX
Dagger.BCast{Base.Broadcast.Broadcasted{Dagger.DaggerBroadcastStyle, Tuple{Base.OneTo{Int64}, Base.OneTo{Int64}}, typeof(+), Tuple{Dagger.AllocateArray{Float64, 2}, Dagger.AllocateArray{Float64, 2}}}, Float64, 2}(100, 100)

julia> DZ = DY .* 3
Dagger.BCast{Base.Broadcast.Broadcasted{Dagger.DaggerBroadcastStyle, Tuple{Base.OneTo{Int64}, Base.OneTo{Int64}}, typeof(*), Tuple{Dagger.BCast{Base.Broadcast.Broadcasted{Dagger.DaggerBroadcastStyle, Tuple{Base.OneTo{Int64}, Base.OneTo{Int64}}, typeof(+), Tuple{Dagger.AllocateArray{Float64, 2}, Dagger.AllocateArray{Float64, 2}}}, Float64, 2}, Int64}}, Float64, 2}(100, 100)

julia> size(DZ)
(100, 100)

julia> DA = fetch(DZ)
Dagger.DArray{Any, 2, typeof(cat)}(100, 100)
```

Now, `DA` is the lazy result of computing `(DX .+ DX) .* 3`. Note that `DArray`
objects are immutable, and operations on them are thus functional
transformations of their input `DArray`.

!!! note
    Support for mutation of `DArray`s is planned for a future release

Additionally, note that we can still call `size` on these lazy `BCast` objects,
as it's clear what the final output's size will be.

```
julia> Dagger.chunks(DA)
2×2 Matrix{Union{Thunk, Dagger.Chunk}}:
 Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(4, 8, 0x0000000000004e20), ThreadProc(4, 1), AnyScope(), true)  …  Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(2, 5, 0x0000000000004e20), ThreadProc(2, 1), AnyScope(), true)
 Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(5, 5, 0x0000000000004e20), ThreadProc(5, 1), AnyScope(), true)     Chunk{Matrix{Float64}, DRef, ThreadProc, AnyScope}(Matrix{Float64}, ArrayDomain{2}((1:50, 1:50)), DRef(3, 3, 0x0000000000004e20), ThreadProc(3, 1), AnyScope(), true)
```

Here we can see the `DArray`'s internal representation of the partitions, which
are stored as Dagger `Chunk` objects (which, as a reminder, may reference data
which exists on other Julia workers). One doesn't typically need to worry about
these internal details unless implementing operators on `DArray`s.

Finally, it's all the same to get the result of this complicated set of
broadcast operations; just use `fetch` to get a `DArray`, and `collect` to get
an `Array`:

```
julia> DA *= 2
Dagger.BCast{Base.Broadcast.Broadcasted{Dagger.DaggerBroadcastStyle, Tuple{Base.OneTo{Int64}, Base.OneTo{Int64}}, typeof(*), Tuple{Dagger.DArray{Any, 2, typeof(cat)}, Int64}}, Any, 2}(100, 100)

julia> fetch(DA)
Dagger.DArray{Any, 2, typeof(cat)}(100, 100)

julia> collect(DA)
100×100 Matrix{Float64}:
 11.6021    9.12356    0.407394  11.2524     4.89022   …   3.26229    1.23314    1.96686    3.04927   3.65649
  3.78571   6.24751    2.74505    8.3009    11.4331        0.336563   9.37329    2.84604    8.52946  10.9168
  3.9987    0.641359   3.1918    11.4368     4.41555       1.12344    5.44424    3.49739    3.32251   8.86685
  7.90953   1.50281    1.91451    4.89621    9.44033       2.97169    9.68018   11.8686     4.74035   8.49143
  1.0611    5.5909    10.364      5.48194    6.821         0.66667    5.33619    5.56166    8.19974   7.02791
  7.47418  11.3061     7.9809     2.34617    7.90996   …   6.30402   10.2203     4.92873    8.22024   7.41224
  7.06002   0.604601  11.6572     4.95498    0.671179      5.42867    8.19648    0.611793  11.9469    1.6628
  2.97898   0.738068   4.44802    5.81322    7.3991        8.71256    2.48281   11.0882    10.9801   11.2464
  1.34064   7.37116    1.14921    3.95358    9.73416       7.83354   10.8357     0.270462   9.93926   9.05206
  8.77125   0.44711   11.7197    11.6632     8.21711       2.20143    5.06451    3.92386    3.90197   4.32807
 10.6201    4.82176    8.4164    10.5457     2.65546   …  10.4681     1.00604    7.05816    6.33214   4.13517
 10.6633   10.2059     7.06543    1.58093    5.33819       7.86821    9.56034    2.37929    4.39098  11.6246
 11.1778    6.76896   10.249     11.3147     9.7838        6.17893    0.433731   0.713574   9.99747   0.570143
  ⋮                                                    ⋱   ⋮
  6.19119  11.027     10.0742     3.51595    0.48755       3.56015    7.43083    0.624126   9.0292    3.04445
  3.38276   5.32876    2.66453    4.08388    6.51538      10.8722     5.14729    3.7499     7.11074  11.3595
  4.10258   0.474511   0.852416   4.79806    5.21663   …   9.96304    5.82279    0.818069   9.85573   8.9645
  6.03249   8.82392    2.14424   10.7512     8.28873       8.32419    2.96016    4.97967    2.52393   2.31372
  7.25826   8.49308    3.90884    3.03783    3.67546       6.63201    5.18839    1.99734    8.51863   8.7656
 11.6969    1.29504    0.745432   0.119002   6.11005       5.3909     2.61199   11.5168     8.25466   2.29896
 10.7       9.66697    2.34518    6.68043    4.09362      11.6484     2.53879    9.95172    3.97177   9.53493
 11.652     3.53655    8.38743    3.75028   11.8518    …   3.11588    1.07276    8.12898    8.80697   1.50331
  9.69158  11.2718     8.98014    2.71964    4.11854       0.840723   4.55286    4.47269    8.30213   0.927262
 10.5868   11.9395     8.22633    6.71811    9.6942        2.2561     0.233772   1.76577    9.67937   8.29349
  9.19925   5.77384    2.18139   10.3563     6.7716        9.8496    11.3777     6.43372   11.2769    4.82911
  9.15905   8.12721   11.1374     6.32082    3.49716       7.23124   10.3995     6.98103    7.72209   6.08033
```
