import Base: filter, map, reduce

"""
    map(f, d::DTable) -> DTable

Applies `f` to each row of `d`.
The applied function needs to return a `Tables.Row` compatible object.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    m = map(x -> (r = x.a + x.b,), d)
    fetch(m)
```
"""
function map(f, d::DTable)
    chunk_wrap = (chunk, f) -> begin
        if isvalid(chunk)
            m = TableOperations.map(f, chunk)
            Tables.materializer(chunk)(m)
        else
            chunk
        end
    end
    chunks = map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks)
    DTable(chunks, d.tabletype)
end

"""
    reduce(f, d::DTable; cols=nothing, [init]) -> NamedTuple

Reduces `d` using function `f` applied on all columns of the DTable.
By providing the kwarg `cols` as a `Vector{Symbol}` object it's possible 
to restrict the reduction to the specified columns.
The reduced values are provided in a NamedTuple under names of reduced columns.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    r1 = reduce(+, d)
    r2 = reduce(+, d, cols=[:a])
    fetch(r1); fetch(r2)
```
"""
function reduce(f, d::DTable; cols=nothing::Union{Nothing, Vector{Symbol}}, init=Base._InitialValue())
    # TODO replace this with checking the colnames in schema, once schema handling gets introduced
    if length(d.chunks) > 0
        columns = isnothing(cols) ? Tables.columnnames(Tables.columns(_retrieve(d.chunks[begin]))) : cols
    else
        return Dagger.@spawn NamedTuple()
    end

    chunk_reduce = (_f, _chunk, _cols, _init) -> begin
        values = [reduce(_f, Tables.getcolumn(_chunk, c); init=_init) for c in _cols]
        (; zip(_cols, values)...)
    end
    chunk_reduce_results = [Dagger.@spawn chunk_reduce(f, c, columns, init)  for c in d.chunks]

    construct_single_column = (_col, _chunk_results...) -> getindex.(_chunk_results, _col)
    result_columns = [Dagger.@spawn construct_single_column(c, chunk_reduce_results...) for c in columns]

    reduce_result_column = (_f, _c, _init) -> reduce(_f, _c; init=_init)
    reduce_chunks = [Dagger.@spawn reduce_result_column(f, c, init) for c in result_columns]

    construct_result = (_cols, _vals...) -> (; zip(_cols, _vals)...)
    Dagger.@spawn construct_result(columns, reduce_chunks...)
end



"""
    filter(f, d::DTable) -> DTable

Filter `d` using `f`.
Returns a filtered `DTable` that can be processed further.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    f = filter(x -> x.a > 0.5, d)
    fetch(f)
```
"""
function filter(f, d::DTable)
    chunk_wrap = (chunk, f) -> TableOperations.filter(f, chunk) |> Tables.materializer(chunk)
    DTable(map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks), d.tabletype)
end
