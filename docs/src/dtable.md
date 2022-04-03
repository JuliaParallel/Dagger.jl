# Distributed table

The `DTable`, or "distributed table", is an abstraction layer on top of Dagger
that allows loading table-like structures into a distributed environment.  The
main idea is that a Tables.jl-compatible source provided by the user gets
partitioned into several parts and stored as `Chunk`s.  These can then be
distributed across worker processes by the scheduler as operations are
performed on the containing `DTable`.

Operations performed on a `DTable` leverage the fact that the table is
partitioned, and will try to apply functions per-partition first, afterwards
merging the results if needed.

The distributed table is backed by Dagger's Eager API (`Dagger.@spawn` and
`Dagger.spawn`).  To provide a familiar usage pattern you can call `fetch` on a
`DTable` instance, which returns an in-memory instance of the underlying table
type (such as a `DataFrame`, `TypedTable`, etc).

## Creating a `DTable`

There are currently two ways of constructing a distributed table:

### Tables.jl source

Provide a `Tables.jl` compatible source, as well as a `chunksize`, which is the
maximum number of rows of each partition:

```julia
julia> using Dagger

julia> table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10]);

julia> d = DTable(table, 2)
DTable with 3 partitions
Tabletype: NamedTuple

julia> fetch(d)
(a = [1, 2, 3, 4, 5], b = [6, 7, 8, 9, 10])
```

### Loader function and file list

Provide a `loader_function` and a list of filenames, which are parts of the
full table:

```julia
julia> using Dagger, CSV

julia> files = ["1.csv", "2.csv", "3.csv"];

julia> d = DTable(CSV.File, files)
DTable with 3 partitions
Tabletype: unknown (use `tabletype!(::DTable)`)

julia> tabletype(d)
NamedTuple

julia> fetch(d)
(a = [1, 2, 1, 2, 1, 2], b = [6, 7, 6, 7, 6, 7])
```

## Underlying table type

The underlying type of the partition is, by default, of the type constructed by
`Tables.materializer(source)`:

```julia
julia> table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10]);

julia> d = DTable(table, 2)
DTable with 3 partitions
Tabletype: NamedTuple

julia> fetch(d)
(a = [1, 2, 3, 4, 5], b = [6, 7, 8, 9, 10])
```

To override the underlying type you can provide a kwarg `tabletype` to the
`DTable` constructor.  You can also choose which tabletype the `DTable` should
be fetched into:

```julia
julia> using DataFrames

julia> table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10]);

julia> d = DTable(table, 2; tabletype=DataFrame)
DTable with 3 partitions
Tabletype: DataFrame

julia> fetch(d)
5×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      6
   2 │     2      7
   3 │     3      8
   4 │     4      9
   5 │     5     10

julia> fetch(d, NamedTuple)
(a = [1, 2, 3, 4, 5], b = [6, 7, 8, 9, 10])
```

# Table operations

**Warning: this interface is experimental and may change at any time**

The current set of operations available consist of three simple functions:
`map`, `filter` and `reduce`.

Below is an example of their usage.

For more information please refer to the API documentation and unit tests.

```julia
julia> using Dagger

julia> d = DTable((k = repeat(['a', 'b'], 500), v = repeat(1:10, 100)), 100)
DTable with 10 partitions
Tabletype: NamedTuple

julia> using DataFrames

julia> m = map(x -> (t = x.k + x.v, v = x.v), d)
DTable with 10 partitions
Tabletype: NamedTuple

julia> fetch(m, DataFrame)
1000×2 DataFrame
  Row │ t     v
      │ Char  Int64
──────┼─────────────
    1 │ b         1
    2 │ d         2
    3 │ d         3
  ⋮   │  ⋮      ⋮
  999 │ j         9
 1000 │ l        10
    995 rows omitted

julia> f = filter(x -> x.t == 'd', m)
DTable with 10 partitions
Tabletype: NamedTuple

julia> fetch(f, DataFrame)
200×2 DataFrame
 Row │ t     v
     │ Char  Int64
─────┼─────────────
   1 │ d         2
   2 │ d         3
  ⋮  │  ⋮      ⋮
 200 │ d         3
   197 rows omitted

julia> r = reduce(+, m, cols=[:v])
EagerThunk (running)

julia> fetch(r)
(v = 5500,)
```

## `mapreduce` usage

The operation `mapreduce` is especially helpful when using `OnlineStats`.
It lets you transform a row to the format you need right before applying the reduce function.
In consequence a lot of memory usage is saved due to the lack of an intermediate `map` step that allocates a full column.

```julia
julia> using Dagger, OnlineStats

julia> fetch(Dagger.mapreduce(sum, fit!, d1, init = Mean()))
Mean: n=100 | value=1.50573

julia> d1 = DTable((a=collect(1:100).%3, b=rand(100)), 25);

julia> gg = GroupBy(Int, Mean());

julia> fetch(Dagger.mapreduce(x-> (x.a, x.b), fit!, d1, init=gg))
GroupBy: Int64 => Mean
├─ 1
│  └─ Mean: n=34 | value=0.491379
├─ 2
│  └─ Mean: n=33 | value=0.555258
└─ 0
    └─ Mean: n=33 | value=0.470984

julia> d2 = DTable((;a1=abs.(rand(Int, 100).%2), [Symbol("a\$(i)") => rand(100) for i in 2:3]...), 25);

julia> gb = GroupBy(Int, Group([Series(Mean(), Variance(), Extrema()) for _ in 1:3]...));

julia> fetch(Dagger.mapreduce(r -> (r.a1, tuple(r...)), fit!, d2, init = gb))
GroupBy: Int64 => Group
├─ 1
│  └─ Group
│     ├─ Series
│     │  ├─ Mean: n=57 | value=1.0
│     │  ├─ Variance: n=57 | value=0.0
│     │  └─ Extrema: n=57 | value=(min = 1.0, max = 1.0, nmin = 57, nmax = 57)
│     ├─ Series
│     │  ├─ Mean: n=57 | value=0.540256
│     │  ├─ Variance: n=57 | value=0.0767802
│     │  └─ Extrema: n=57 | value=(min = 0.0132545, max = 0.996059, nmin = 1, nmax = 1)
│     └─ Series
│        ├─ Mean: n=57 | value=0.536187
│        ├─ Variance: n=57 | value=0.0981499
│        └─ Extrema: n=57 | value=(min = 0.0112471, max = 0.991461, nmin = 1, nmax = 1)
└─ 0
   └─ Group
      ├─ Series
      │  ├─ Mean: n=43 | value=0.0
      │  ├─ Variance: n=43 | value=0.0
      │  └─ Extrema: n=43 | value=(min = 0.0, max = 0.0, nmin = 43, nmax = 43)
      ├─ Series
      │  ├─ Mean: n=43 | value=0.459732
      │  ├─ Variance: n=43 | value=0.0911548
      │  └─ Extrema: n=43 | value=(min = 0.000925526, max = 0.962072, nmin = 1, nmax = 1)
      └─ Series
         ├─ Mean: n=43 | value=0.490613
         ├─ Variance: n=43 | value=0.0850503
         └─ Extrema: n=43 | value=(min = 0.0450505, max = 0.981091, nmin = 1, nmax = 1)
```


# Dagger.groupby interface

A `DTable` can be grouped which will result in creation of a `GDTable`.
A distinct set of values contained in a single or multiple columns can be used as grouping keys.
If a transformation of a row needs to be performed in order to obtain the grouping key there's
also an option to provide a custom function returning a key, which is applied per row.

The set of keys the `GDTable` is grouped by can be obtained using
the `keys(gd::GDTable)` function. To get a fragment of the `GDTable` containing
records belonging under a single key the `getindex(gd::GDTable, key)` function can be used.

```julia
julia> d = DTable((a=shuffle(repeat('a':'d', inner=4, outer=4)),b=repeat(1:4, 16)), 4)
DTable with 16 partitions
Tabletype: NamedTuple

julia> Dagger.groupby(d, :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> Dagger.groupby(d, [:a, :b])
GDTable with 16 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> Dagger.groupby(d, row -> row.a + row.b)
GDTable with 7 partitions and 7 keys
Tabletype: NamedTuple
Grouped by: #5

julia> g = Dagger.groupby(d, :a); keys(g)
KeySet for a Dict{Char, Vector{UInt64}} with 4 entries. Keys:
  'c'
  'd'
  'a'
  'b'

julia> g['c']
DTable with 1 partitions
Tabletype: NamedTuple
```

## GDTable operations

Operations such as `map`, `filter`, `reduce` can be performed on a `GDTable`

```julia
julia> g = Dagger.groupby(d, [:a, :b])
GDTable with 16 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> f = filter(x -> x.a != 'd', g)
GDTable with 16 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> trim!(f)
GDTable with 12 partitions and 12 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> m = map(r -> (a = r.a, b = r.b, c = r.b .- 3), f)
GDTable with 12 partitions and 12 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> r = reduce(*, m)
EagerThunk (running)

julia> DataFrame(fetch(r))
12×5 DataFrame
 Row │ a     b      result_a  result_b  result_c 
     │ Char  Int64  String    Int64     Int64    
─────┼───────────────────────────────────────────
   1 │ a         1  aaaa             1        16
   2 │ c         3  ccc             27         0
   3 │ a         3  aa               9         0
   4 │ b         4  bbbb           256         1
   5 │ c         4  cccc           256         1
   6 │ b         2  bbbb            16         1
   7 │ b         1  bbbb             1        16
   8 │ a         2  aaa              8        -1
   9 │ a         4  aaaaaaa      16384         1
  10 │ b         3  bbbb            81         0
  11 │ c         2  ccccc           32        -1
  12 │ c         1  cccc             1        16
```

## Iterating over a GDTable

`GDTable` can be iterated over and each element returned will be a pair of key
and a `DTable` containing all rows associated with that grouping key.

```julia
julia> d = DTable((a=repeat('a':'b', inner=2),b=1:4), 2)
DTable with 2 partitions
Tabletype: NamedTuple

julia> g = Dagger.groupby(d, :a)
GDTable with 2 partitions and 2 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> for (key, dt) in g
           println("Key: $key")
           println(fetch(dt, DataFrame))
       end
Key: a
2×2 DataFrame
 Row │ a     b     
     │ Char  Int64 
─────┼─────────────
   1 │ a         1
   2 │ a         2
Key: b
2×2 DataFrame
 Row │ a     b     
     │ Char  Int64 
─────┼─────────────
   1 │ b         3
   2 │ b         4
```

# Joins

There are two join methods available currently: `leftjoin` and `innerjoin`.
The interface is aiming to be compatible with the `DataFrames.jl` join interface, but for now it only supports
the `on` keyword argument with symbol input. More keyword arguments known from `DataFrames` may be introduced in the future.

It's possible to perform a join on a `DTable` and any `Tables.jl` compatible table type.
Joining two `DTable`s is also supported and it will leverage the fact that the second `DTable` is partitioned during the joining process.

There are several options to make your joins faster by providing additional information about the tables.
It can be done by using the following keyword arguments:

- `l_sorted`: To indicate the left table is sorted - only useful if the `r_sorted` is set to `true` as well.
- `r_sorted`: To indicate the right table is sorted.
- `r_unique`: To indicate the right table only contains unique keys.
- `lookup`: To provide a dict-like structure that will allow for quicker matching of inner rows. The structure needs to contain keys in form of a `Tuple` of the matched columns and values in form of type `Vector{UInt}` containing the related row indices.

Currently there is a special case available where joining a `DTable` (with `DataFrame` as the underlying table type) with a `DataFrame` will use
the join functions coming from the `DataFrames.jl` package for the per chunk joins.
In the future this behavior will be expanded to any type that implements its own join methods, but for now is limited to `DataFrame` only.

Please note that the usage of any of the keyword arguments described above will always result in the usage of generic join methods
defined in `Dagger` regardless of the availability of specialized methods.

```julia
julia> using Tables; pp = d -> for x in Tables.rows(d) println("$(x.a), $(x.b), $(x.c)") end;

julia> d1 = (a=collect(1:6), b=collect(1:6));

julia> d2 = (a=collect(2:5), c=collect(-2:-1:-5));

julia> dt = DTable(d1, 2)
DTable with 3 partitions
Tabletype: NamedTuple

julia> pp(leftjoin(dt, d2, on=:a))
2, 2, -2
1, 1, missing
3, 3, -3
4, 4, -4
5, 5, -5
6, 6, missing

julia> pp(innerjoin(dt, d2, on=:a))
2, 2, -2
3, 3, -3
4, 4, -4
5, 5, -5
```