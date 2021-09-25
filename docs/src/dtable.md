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

# Dagger.groupby interface

A `DTable` can be grouped which will result in creation of a `GDTable`.
It can be grouped using a distinct set of values contained in a single or multiple columns.
A user provided function can be used for grouping in case a row needs
to be transformed in order to obtain a grouping key.

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
Grouped by: custom function
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

`GDTable` can be iterated over and each element will be a pair of key and `DTable` with all rows associated with that key.

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

# API

```@docs
DTable
tabletype
tabletype!
trim
trim!
map
filter
reduce
groupby
```
