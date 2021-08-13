import Base: filter, map, reduce

"""
    map(f, d::DTableRows)

Applies `f` to each row of `d`.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    m = map(x -> x.a + x.b, eachrow(d))
    fetch(m)
```
"""
function map(f, d::DTableRows)
    chunk_wrap = (chunk, f) -> begin
        if length(Tables.rows(chunk)) > 0
            m = TableOperations.map(x -> (__map_result = f(x),), chunk)
            res = Tables.materializer(chunk)(m)
        else # chunk empty
            res = Tables.materializer(chunk)((__map_result = [],))
        end
    end
    chunks = map(c -> Dagger.spawn(chunk_wrap, c, f), d.dtable.chunks)
    DTable(chunks, d.dtable.tabletype)
end

"""
    reduce(f_reduce::Function, f_row::Function, d::DTableRows; init)

Reduces `d` using function `f_reduce` on values obtained from applying `f_row` on its rows.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    r = reduce(+, row -> row.a + row.b, eachrow(d); init=0.0)
    fetch(r)
```
"""
function reduce(f, d::DTable; cols=nothing::Union{Nothing, Vector{Symbol}}, init=Base._InitialValue())
    columns = isnothing(cols) ? Tables.columnnames(Tables.columns(fetch(d.chunks[1]))) : cols
    # todo replace this tables.columns with some schema query

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
    filter(f, d::DTable)

Filter `d` using `f`.
Returns a filtered `DTable` that can be processed further.

# Examples
```
    d = DTable((a=rand(1000), b=rand(1000)), chunksize=100)
    f = filter(x -> x.a > 0.5, eachrow(d))
    fetch(f)
```
"""
function filter(f, d::DTable)
    chunk_wrap = (chunk, f) -> TableOperations.filter(f, chunk) |> Tables.materializer(chunk)
    DTable(map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks), d.tabletype)
end
