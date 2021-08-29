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
    col_in_chunk_reduce = (_f, _c, _init, _chunk) -> reduce(_f, Tables.getcolumn(_chunk, _c); init=deepcopy(_init))

    chunk_reduce = (_f, _chunk, _cols, _init) -> begin
        # TODO: potential speedup enabled by commented code below by reducing the columns in parallel
        v = [col_in_chunk_reduce(_f, c, _init, _chunk) for c in _cols]
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


function _temp(d::DTable, col; npartitions=-1)
    partition_heuristic = 2 * length(d.chunks)
    if npartitions < 0 
        npartitions = partition_heuristic()
    end
    e = fetch(Dagger.extrema(d, col))

    parts = range(e[1], e[2], length=npartitions)

    chunk_wrap = (chunk, l, r)  -> begin
        if l === nothing
            chunk[(getindex(chunk, :, col) .<= r),:]
        elseif r === nothing
            chunk[(l .< getindex(chunk, :, col)),:]
        else
            chunk[(l .< getindex(chunk, :, col)) .& (getindex(chunk, :, col) .<= r),:]
        end
    end
    v = Vector{Dagger.EagerThunk}()
    

    intervals = [
        (nothing, parts[begin + 1]),
        [(parts[i], parts[i + 1]) for i in 2:length(parts) - 2]...,
        (parts[end-1], nothing)
    ]

    index = Dict{eltype(intervals), Vector{Int}}()


    for i in intervals
        l = length(v)
        append!(v, [Dagger.@spawn chunk_wrap(c, i[1], i[2]) for c in d.chunks])
        index[i] = collect(l + 1:length(v))
    end
    
    squash = (chunks...) -> vcat(chunks...)
    v2 = Vector{Dagger.EagerThunk}()

    for k in enumerate(keys(index))
        c = getindex.(Ref(v), index[k[2]])
        push!(v2, Dagger.@spawn squash(c...))
        index[k[2]] = [k[1]]
    end

    DTable(v2, d.schema)
end


function groupby(d::DTable, col)
    distinct_values = (_chunk, _col) -> unique(Tables.getcolumn(_chunk, _col))

    filter_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end

    chunk_wrap = (_chunk, _col) -> begin
        vals = distinct_values(_chunk, _col)
        if length(vals) > 1
            [v => Dagger.@spawn filter_wrap(_chunk, x -> Tables.getcolumn(x, _col) .== v) for v in vals]
        else
            [vals[1] => Dagger.@spawn (x->x)(_chunk)]
        end
    end

    v = [Dagger.@spawn chunk_wrap(c, col) for c in d.chunks]

    build_index = (col, vs...) -> begin
        v = vcat(vs...)
        keys = unique(map(x-> x[1], v))
        idx = Dict([k => Vector{Int}() for k in keys])
        for (i, e) in enumerate(map(x-> x[1], v))
            push!(idx[e], i) 
        end
        idx, map(x-> x[2], v)
    end

    res = Dagger.@spawn build_index(v...)
    r = fetch(res)
    DTable(r[2], d.tabletype, Dict(col => r[1])) 
end
