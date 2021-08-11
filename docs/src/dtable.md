# Distributed table

The distributed table is an abstraction layer on top of `Dagger` that allows
loading table-like structures into a distributed environment.
The main idea is that a `Tables.jl` compatible source provided by the user
gets partitioned into several parts and stored as `Chunks` (or `Thunks`).
These can be then distributed across worker processes according to scheduler's logic.

Operations performed on a `DTable` leverage the fact that the table is partitioned 
and will try to apply functions per-partition first and then merge the result if needed.

There are currently two ways of constructing a distributed table:

1. By providing a `Tables.jl` compatible source and the `chunksize`, 
which is the size of the partition measured in row count.
2. By providing a `loader_function` and a list of filenames, which are parts of the full table.

```julia
```