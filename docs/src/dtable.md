# Distributed table

The distributed table is an abstraction layer on top of `Dagger` that allows
loading table-like structures into a distributed environment.
The main idea is that a `Tables.jl` compatible source provided by the user
gets partitioned into several parts and stored as `Chunks` (or `Thunks`).
These can be then distributed across worker processes according to scheduler's logic.

Operations performed on a `DTable` leverage the fact that the table is partitioned 
and will try to apply functions per-partition first and then merge the result if needed.

## Creating a DTable

There are currently two ways of constructing a distributed table:

1. By providing a `Tables.jl` compatible source and the `chunksize`, 
which is the size of the partition measured in row count.
2. By providing a `loader_function` and a list of filenames, which are parts of the full table.

The underlying type of the partition is by default of type constructed by `Tables.materializer(source)`.
To override the underlying type you can provide a kwarg `tabletype` to the constructor.

```julia
table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
d = DTable(table, 2)
```

```julia
using CSV
files = ["1.csv", "2.csv", "3.csv"]
d = DTable(CSV.File, files)
tabletype(d)


using DataFrames
table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
d = DTable(table, 2; tabletype=DataFrame)
fetch(d)
fetch(d, NamedTuple)

```

## Available operations

** Warning: the interface is experimental and may change from version to version **
The current set of operations available consist of three simple functions `map`, `filter` and `reduce`.

Below an example of their usage.

```julia
table = (a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
d = DTable(table, 2)

f = filter(row -> row.a > 2)

m = map(row -> row.a + row.b, f)

r = reduce(+, row-> row.a, f)

fetch(f)
fetch(m)
fetch(r)

```
