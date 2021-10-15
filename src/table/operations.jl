import Base: filter, map, reduce

"""
    map(f, d::DTable) -> DTable

Applies `f` to each row of `d`.
The applied function needs to return a `Tables.Row` compatible object (e.g. `NamedTuple`).

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> m = map(x -> (r = x.a + x.b,), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r = [2, 3, 4],)

julia> m = map(x -> (r1 = x.a + x.b, r2 = x.a - x.b), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r1 = [2, 3, 4], r2 = [0, 1, 2])
```
"""
function map(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        if isnonempty(_chunk)
            m = TableOperations.map(_f, _chunk)
            Tables.materializer(_chunk)(m)
        else
            _chunk
        end
    end
    chunks = map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks)
    DTable(chunks, d.tabletype)
end


"""
    map(f, gd::GDTable) -> GDTable

Applies `f` to each row of `gd`.
The applied function needs to return a `Tables.Row` compatible object (e.g. `NamedTuple`).

# Examples
```julia
julia> g = Dagger.groupby(DTable((a=repeat('a':'c', inner=2),b=1:6), 2), :a)
GDTable with 3 partitions and 3 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> m = map(r -> (a = r.a, b = r.b, c = r.a + r.b), g)
GDTable with 3 partitions and 3 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(m)
(a = ['a', 'a', 'c', 'c', 'b', 'b'], b = [1, 2, 5, 6, 3, 4], c = ['b', 'c', 'h', 'i', 'e', 'f'])
```
"""
function map(f, gd::GDTable)
    d = map(f, gd.dtable)
    GDTable(d, gd.cols, gd.index)
end

"""
    reduce(f, d::DTable; cols=nothing, [init]) -> NamedTuple

Reduces `d` using function `f` applied on all columns of the DTable.

By providing the kwarg `cols` as a `Vector{Symbol}` object it's possible
to restrict the reduction to the specified columns.
The reduced values are provided in a NamedTuple under names of reduced columns.

For the `init` kwarg please refer to `Base.reduce` documentation,
as it follows the same principles. 

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> r1 = reduce(+, d)
EagerThunk (running)

julia> fetch(r1)
(a = 6, b = 3)

julia> r2 = reduce(*, d, cols=[:a])
EagerThunk (running)

julia> fetch(r2)
(a = 6,)
```
"""
function reduce(f, d::DTable; cols=nothing::Union{Nothing, Vector{Symbol}}, init=Base._InitialValue())
    # TODO replace this with checking the colnames in schema, once schema handling gets introduced
    if length(d.chunks) > 0
        columns = isnothing(cols) ? Tables.columnnames(Tables.columns(_retrieve(d.chunks[1]))) : cols
    else
        return Dagger.@spawn NamedTuple()
    end
    col_in_chunk_reduce = (_f, _c, _init, _chunk) -> reduce(_f, Tables.getcolumn(_chunk, _c); init=_init)

    chunk_reduce = (_f, _chunk, _cols, _init) -> begin
        # TODO: potential speedup enabled by commented code below by reducing the columns in parallel
        v = [col_in_chunk_reduce(_f, c, deepcopy(_init), _chunk) for c in _cols]
        (; zip(_cols, v)...)

        # TODO: uncomment and define a good threshold for parallelization when this get's resolved
        # https://github.com/JuliaParallel/Dagger.jl/issues/267
        # This piece of code (else option) below is causing the issue above
        # when reduce is repeatedly executed or @btime is used.
        # if length(_cols) <= 1
        #     v = [col_in_chunk_reduce(_f, c, _init, _chunk) for c in _cols]
        # else
        #     values = [Dagger.spawn(col_in_chunk_reduce, _f, c, _init, _chunk) for c in _cols]
        #     v = fetch.(values)
        # end
        # (; zip(_cols, v)...)
    end
    chunk_reduce_results = [Dagger.@spawn chunk_reduce(f, c, columns, deepcopy(init)) for c in d.chunks]

    construct_single_column = (_col, _chunk_results...) -> getindex.(_chunk_results, _col)
    result_columns = [Dagger.@spawn construct_single_column(c, chunk_reduce_results...) for c in columns]

    reduce_result_column = (_f, _c, _init) -> reduce(_f, _c; init=_init)
    reduce_chunks = [Dagger.@spawn reduce_result_column(f, c, deepcopy(init)) for c in result_columns]

    construct_result = (_cols, _vals...) -> (; zip(_cols, _vals)...)
    Dagger.@spawn construct_result(columns, reduce_chunks...)
end

"""
    reduce(f, gd::GDTable; cols=nothing, prefix="result_" [init]) -> EagerThunk -> NamedTuple

Reduces `gd` using function `f` applied on all columns of the DTable.
Returns results per group in columns with names prefixed with the `prefix` kwarg.
For more information on kwargs see `reduce(f, d::DTable)`

# Examples
```julia
julia> g = Dagger.groupby(DTable((a=repeat('a':'d', inner=2),b=1:8), 2), :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(reduce(*, g))
(a = ['a', 'c', 'd', 'b'], result_a = ["aa", "cc", "dd", "bb"], result_b = [2, 30, 56, 12])
```
"""
function reduce(
    f,
    gd::GDTable;
    cols=nothing::Union{Nothing, Vector{Symbol}},
    prefix::String="result_",
    init=Base._InitialValue())

    construct_result = (_keys, _results...) -> begin
        result_cols = keys(first(_results))
        k = [col => getindex.(_keys, i) for (i, col) in enumerate(grouped_cols(gd))]
        r = [Symbol(prefix * string(r)) => collect(getindex.(_results, r)) for r in result_cols]
        (;k...,r...)
    end
    Dagger.@spawn construct_result(keys(gd), [reduce(f, d[2]; cols=cols, init=deepcopy(init)) for d in gd]...)
end

"""
    filter(f, d::DTable) -> DTable

Filter `d` using `f`.
Returns a filtered `DTable` that can be processed further.

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> f = filter(x -> x.a < 3, d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])

julia> f = filter(x -> (x.a < 3) .& (x.b > 0), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])
```
"""
function filter(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end
    DTable(map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks), d.tabletype)
end


"""
    filter(f, gd::GDTable) -> GDTable

Filter 'gd' using 'f', returning a filtered `GDTable`.
Calling `trim!` on a filtered `GDTable` will clean up the empty keys and partitions.

# Examples
```julia
julia> g = Dagger.groupby(DTable((a=repeat('a':'d', inner=2),b=1:8), 2), :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> f = filter(x -> x.a ∈ ['a', 'b'], g)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> fetch(f)
(a = ['a', 'a', 'b', 'b'], b = [1, 2, 3, 4])

julia> trim!(f)
GDTable with 2 partitions and 2 keys
Tabletype: NamedTuple
Grouped by: [:a]
```
"""
function filter(f, gd::GDTable)
    d = filter(f, gd.dtable)
    GDTable(d, gd.cols, gd.index)
end
