"""
    groupby(d::DTable, col::Symbol; merge=true, chunksize=0) -> GDTable

Groups `d` by distinct values of column `col`.

The process of grouping can be affected by providing kwargs `merge` and `chunksize`.
By default all the chunks belonging to a single key will be merged into a single partition.
Providing a positive value in `chunksize` will attempt to merge the smaller partitions
into partitions not bigger than `chunksize`. Please note that partitions bigger than `chunksize`
will not be split into partitions of `chunksize`.
Merging can be disabled completely by providing `merge=false`.

# Examples
```julia
julia> d = DTable((a=shuffle(repeat('a':'d', inner=4, outer=4)),), 4)
DTable with 16 partitions
Tabletype: NamedTuple

julia> Dagger.groupby(d, :a)
GDTable with 4 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> Dagger.groupby(d, :a, chunksize=3)
GDTable with 24 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]

julia> Dagger.groupby(d, :a, merge=false)
GDTable with 42 partitions and 4 keys
Tabletype: NamedTuple
Grouped by: [:a]
```
"""
function groupby(d::DTable, col::Symbol; merge=true, chunksize=0)
    rowmap = (_row, _col) -> Tables.getcolumn(_row, _col)
    rowmap_w_cols = _row -> rowmap(_row, col)
    _groupby(d, rowmap_w_cols, [col], merge, chunksize)
end

"""
    groupby(d::DTable, cols::Vector{Symbol}; merge=true, chunksize=0)

Groups the `d` by distinct values of columns `cols`.
The key is constructed by creating a NamedTuple from each row based on `cols` provided.

For kwargs usage details see `groupby(d::DTable, col::Symbol)`

# Examples
```julia
julia> d = DTable((a=shuffle(repeat('a':'d', inner=4, outer=4)),b=repeat(1:4, 16)), 4)
DTable with 16 partitions
Tabletype: NamedTuple

julia> Dagger.groupby(d, [:a,:b])
GDTable with 16 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> Dagger.groupby(d, [:a,:b], chunksize=3)
GDTable with 27 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]

julia> Dagger.groupby(d, [:a,:b], merge=false)
GDTable with 64 partitions and 16 keys
Tabletype: NamedTuple
Grouped by: [:a, :b]
```
"""
function groupby(d::DTable, cols::Vector{Symbol}; merge=true, chunksize=0)
    rowmap = (_row, _cols) -> (;[c => Tables.getcolumn(_row, c) for c in _cols]...)
    rowmap_w_cols = _row -> rowmap(_row, cols)
    _groupby(d, rowmap_w_cols, cols, merge, chunksize)
end

"""
    groupby(d::DTable, f::Function; merge=true, chunksize=0)

Groups `d` by the distinct set of keys created by applying `f` to each row in `d`.

For kwargs usage details see `groupby(d::DTable, col::Symbol)`

```julia
julia> d = DTable((a=shuffle(repeat('a':'d', inner=4, outer=4)),b=repeat(1:4, 16)), 4)
DTable with 16 partitions
Tabletype: NamedTuple

julia> function group_fun(row)
           row.a + row.b
       end
group_fun (generic function with 1 method)

julia> Dagger.groupby(d, group_fun)
GDTable with 7 partitions and 7 keys
Tabletype: NamedTuple
Grouped by: group_fun

julia> Dagger.groupby(d, row -> row.a + row.b, chunksize=3)
GDTable with 25 partitions and 7 keys
Tabletype: NamedTuple
Grouped by: group_fun

julia> Dagger.groupby(d, row -> row.a + row.b, merge=false)
GDTable with 52 partitions and 7 keys
Tabletype: NamedTuple
Grouped by: group_fun
```
"""
groupby(d::DTable, f::Function; merge=true, chunksize=0) = _groupby(d, f, nothing, merge, chunksize)

"""
    _groupby(d::DTable, row_function::Function, cols::Union{Nothing, Vector{Symbol}}, merge::Bool, chunksize::Int)

Internal function for performing the groupby steps based on common arguments.
"""
function _groupby(
    d::DTable,
    row_function::Function,
    cols::Union{Nothing, Vector{Symbol}},
    merge::Bool,
    chunksize::Int)

    grouping_function = isnothing(cols) ? row_function : nothing 

    v = [Dagger.@spawn distinct_partitions(c, row_function) for c in d.chunks]

    index, chunks = fetch(Dagger.@spawn build_groupby_index(merge, chunksize, tabletype(d), v...))
    GDTable(DTable(VTYPE(chunks), d.tabletype), cols, index, grouping_function)
end

"""
    distinct_partitions(chunk, f::Function)

Takes a partition and groups its rows according based on the key value returned by `f`.
"""
function distinct_partitions(chunk, f::Function)
    rows = Tables.rows(chunk)
    keyval = f(iterate(rows)[1])

    _distinct_partitions_iterate(chunk, f, keyval)
end

function _distinct_partitions_iterate(chunk, f, keyval::T) where T
    rows = Tables.rows(chunk)
    acc = Dict{T, Vector{eltype(rows)}}()

    for row in rows
        key = convert(T, f(row))
        v = get!(acc, key, Vector{eltype(rows)}())
        push!(v, row)
    end
    
    Vector{Pair{T, Chunk}}([x => Dagger.tochunk(Tables.columntable(acc[x])) for x in collect(keys(acc))])
end


merge_chunks(sink, chunks...) = sink(TableOperations.joinpartitions(Tables.partitioner(identity, chunks)))

rowcount(chunk) = length(Tables.rows(chunk))

"""
    build_groupby_index(merge::Bool, chunksize::Int, tabletype, vs...)

Takes the intermediate result of `distinct_partitions` and builds an index.
Merges partitions if possible according to the `chunksize` provided.
It will only merge chunks if their length after merging is `<= chunksize`.
It doesn't split chunks larger than `chunksize` and small chunks
may be leftover after merging if no appropriate pair was found.
"""
function build_groupby_index(merge::Bool, chunksize::Int, tabletype, vs...)
    v = vcat(vs...)
    @assert typeof(v) <: Vector
    @assert eltype(v) <: Pair

    keytype = eltype(v).types[1]

    chunks = Vector{Chunk}(map(x -> x[2], v))

    idx = Dict{keytype, Vector{UInt}}()
    for (i, k) in enumerate(map(x -> x[1], v))
        get!(idx, k, Vector{UInt}())
        push!(idx[k], i)
    end

    if merge && chunksize <= 0 # merge all partitions into one
        sink = Tables.materializer(tabletype())
        merged_chunks = Vector{Union{EagerThunk, Chunk}}()
        sizehint!(merged_chunks, length(keys(idx)))

        for (i, k) in enumerate(keys(idx))
            c = getindex.(Ref(chunks), idx[k])
            push!(merged_chunks, length(c) == 1 ? first(c) : Dagger.@spawn merge_chunks(sink, c...))
            idx[k] = [i]
        end
        return idx, merged_chunks

    elseif merge && chunksize > 0 # merge all but try to merge all the small chunks into chunks of chunksize
        sink = Tables.materializer(tabletype())
        merged_chunks = Vector{Union{EagerThunk, Chunk}}()

        all_lengths = [Dagger.@spawn rowcount(c) for c in chunks]

        for k in keys(idx)
            _indices = idx[k]
            _lengths = fetch.(getindex.(Ref(all_lengths), _indices))

            ord = sortperm(_lengths, rev=true) # sorting indices and lengths by lengths
            _indices .= _indices[ord]
            _lengths .= _lengths[ord]

            l = 1
            r = length(_indices)
            prev_r = r

            while l <= r
                if _lengths[l] >= chunksize || # chunk already bigger than minimum, so move forward
                    _lengths[l] + _lengths[r] > chunksize || # potential merge would be bigger, so move forward
                    l == r # last iteration to push last chunk and trigger the last merge

                    _chunk = chunks[_indices[l]]
                    if r < prev_r # only condition for merging
                        chunks_to_merge = getindex.(Ref(chunks), _indices[r+1:prev_r])
                        _chunk = Dagger.@spawn merge_chunks(sink, _chunk, chunks_to_merge...)
                        prev_r = r
                    end
                    push!(merged_chunks, _chunk)
                    _indices[l] = length(merged_chunks)
                    l += 1
                elseif _lengths[l] + _lengths[r] <= chunksize # merge possible, mark r for merging
                    _lengths[l] += _lengths[r]
                    r -= 1
                end
            end
            idx[k] = _indices[1:r]
        end
        return idx, merged_chunks

    else # no merge
        return idx, chunks
    end
end
