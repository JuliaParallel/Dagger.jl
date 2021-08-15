# Distributed table

The distributed table is an abstraction layer on top of `Dagger` that allows
loading table-like structures into a distributed environment.
The main idea is that a `Tables.jl` compatible source provided by the user
gets partitioned into several parts and stored as `Chunks` (or `Thunks`).
These can be then distributed across worker processes according to scheduler's logic.

Operations performed on a `DTable` leverage the fact that the table is partitioned 
and will try to apply functions per-partition first and then merge the result if needed.

The distributed table is backed by Dagger's Eager API (`Dagger.@spawn` and `Dagger.spawn`).
To provide a familiar usage pattern you can call `fetch` on a DTable instance
even though it is not a `Dagger.EagerThunk` object.

## Creating a DTable

There are currently two ways of constructing a distributed table:

### Tables.jl source

By providing a `Tables.jl` compatible source and the `chunksize`, which is the size of the partition measured in row count.

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

By providing a `loader_function` and a list of filenames, which are parts of the full table.

```julia
julia> using Dagger, CSV

julia> files = ["1.csv", "2.csv", "3.csv"];

julia> d = DTable(CSV.File, files)
DTable with 3 partitions
Tabletype: unknown (use `tabletype(::DTable)`)


julia> tabletype(d)
NamedTuple

julia> fetch(d)
(a = [1, 2, 1, 2, 1, 2], b = [6, 7, 6, 7, 6, 7])
```

## Underlying table type

The underlying type of the partition is by default of type constructed by `Tables.materializer(source)`.

```julia
julia> table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10]);

julia> d = DTable(table, 2)
DTable with 3 partitions
Tabletype: NamedTuple


julia> fetch(d)
(a = [1, 2, 3, 4, 5], b = [6, 7, 8, 9, 10])
```

To override the underlying type you can provide a kwarg `tabletype` to the constructor.
You can also choose which tabletype should the `DTable` be fetched into.

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

**Warning: the interface is experimental and may change at any time**

The current set of operations available consist of three simple functions `map`, `filter` and `reduce`.

Below is an example of their usage.

For more information please refer to the API documentation and test cases.

```julia
julia> using Dagger

julia> d = DTable((k = repeat(['a', 'b'], 500), v = repeat(1:10, 100)), 100)
DTable with 10 partitions
Tabletype: NamedTuple

julia> m = map(x -> (t = x.k + x.v, v = x.v), d)
DTable with 10 partitions
Tabletype: NamedTuple

julia> f = filter(x -> x.t == 'd', m)
DTable with 10 partitions
Tabletype: NamedTuple

julia> using DataFrames; fetch(f, DataFrame)

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

julia> fetch(ans)
(v = 5500,)
```

# API

```@docs
DTable
tabletype
map
filter
reduce
```
