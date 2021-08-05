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
        res = chunk |> TableOperations.map(x-> (result=f(x),)) |> Tables.materializer(chunk)
        Tables.getcolumn(res, :result)
    end
    result_vector = map(c -> Dagger.spawn(chunk_wrap, c, f), d.dtable.chunks)
    Dagger.@spawn vcat(result_vector...)
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
function reduce(f_reduce::Function, f_row::Function, d::DTableRows; init)
    chunk_results = map(c -> Dagger.spawn(_chunk_reduce, f_reduce, f_row, c, init), d.dtable.chunks)
    reduce_chunks = (f, _init, c_res...) -> reduce(f, c_res; init=_init)
    Dagger.@spawn reduce_chunks(f_reduce, init, chunk_results...)
end

function _chunk_reduce(f_reduce, f_row, chunk, init)
    mapped = chunk |> TableOperations.map(x -> (result = f_row(x)))
    reduce(f_reduce, mapped; init=init)
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
