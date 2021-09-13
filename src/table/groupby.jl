function groupby(d::DTable, col::Symbol; merge=true, chunksize=0)
    distinct_values = (_chunk, _col) -> unique(Tables.getcolumn(_chunk, _col))

    filter_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end

    create_distinct_partitions = (_chunk, _col) -> begin
        vals = distinct_values(_chunk, _col)
        if length(vals) > 1
            [v => Dagger.@spawn filter_wrap(_chunk, x -> Tables.getcolumn(x, _col) .== v) for v in vals]
        else
            [first(vals) => Dagger.spawn(identity, _chunk)]
        end
    end

    #v = [create_distinct_partitions(Dagger._retrieve(c), col) for c in d.chunks]
    v = [Dagger.@spawn create_distinct_partitions(c, col) for c in d.chunks]

    #ret = build_groupby_index(merge, chunksize, tabletype(d), fetch.(v)...)
    # Commented spawn version due to instability
    ret = fetch(Dagger.@spawn build_groupby_index(merge, chunksize, tabletype(d), v...))
    DTable(VTYPE(ret[2]), d.tabletype, Dict(col => ret[1])) 
end


function groupby(d::DTable, f::Function; merge=true, chunksize=0)

    filter_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end

    chunk_wrap = (_chunk, _f) -> begin

        # this is faster
        distinct = unique(Tables.getcolumn(Tables.columntable(TableOperations.map(x->(r = _f(x),), _chunk)), :r))
        r = [k => Dagger.spawn(filter_wrap, _chunk, (x) -> _f(x) == k) for k in distinct]

        # rows = Tables.rows(_chunk)
        # it = iterate(rows)
        # vals = nothing
        # if it !== nothing
        #     vals = Dict{typeof(_f(it[1])), Vector{eltype(rows)}}()
        # else
        #     return []
        # end

        # for row in rows
        #     k = _f(row)
        #     if !haskey(vals, k) 
        #        vals[k] = Vector{eltype(rows)}()
        #     end
        #     push!(vals[k], row)
        # end

        # collect_chunk = (rows) -> _sink(Tables.columntable(rows))
        # m = map(k -> k => Dagger.spawn(collect_chunk, vals[k]), collect(keys(vals)))
        # # @assert length(m) == length(r)
        # m
    end

    #v = [chunk_wrap(Dagger._retrieve(c), f, sink) for c in d.chunks]
    v = [Dagger.@spawn chunk_wrap(c, f) for c in d.chunks]


    #ret = build_groupby_index(merge, chunksize, tabletype(d), fetch.(v)...)
    # Commented out spawn version due to instability
    ret = fetch(Dagger.@spawn build_groupby_index(merge, chunksize, tabletype(d), v...))
    DTable(VTYPE(ret[2]), d.tabletype, Dict(:f => ret[1])) 
end

function build_groupby_index(merge::Bool, chunksize::Int, tabletype, vs...)
    v = vcat(vs...)

    ks = unique(map(x-> x[1], v))
    chunks = Vector{EagerThunk}(map(x-> x[2], v))

    idx = Dict([k => Vector{Int}() for k in ks])
    for (i, k) in enumerate(map(x-> x[1], v))
        push!(idx[k], i) 
    end

    if merge && chunksize <= 0 # merge all partitions into one
        sink = Tables.materializer(tabletype())
        merged_chunks = Vector{EagerThunk}()
        sizehint!(merged_chunks, length(keys(idx)))

        for (i, k) in enumerate(keys(idx))
            c = getindex.(Ref(chunks), idx[k])
            push!(merged_chunks, Dagger.@spawn merge_chunks(sink, c...))
            idx[k] = [i]
        end
        return idx, merged_chunks
    elseif merge && chunksize > 0 # merge all but try to merge all the small chunks into chunks of chunksize
        sink = Tables.materializer(tabletype())
        merged_chunks = Vector{EagerThunk}()

        for k in keys(idx)
            _indices = idx[k]
            _chunks = getindex.(Ref(chunks), _indices)
            _lengths = fetch.(Dagger.spawn.(rowcount, _chunks))

            ord = sortperm(_lengths, rev=true) # sorting indices and lengths by lengths
            _indices .= _indices[ord] 
            _lengths .= _lengths[ord]

            l = 1
            r = length(_indices)
            prev_r = r

            while l <= r
                if _lengths[l] >= chunksize || _lengths[l] + _lengths[r] > chunksize || l == r # conditions to move l forward
                    _chunk = chunks[_indices[l]]
                    if r < prev_r # only condition for merging
                        _chunk = Dagger.@spawn merge_chunks(sink, _chunk, getindex.(Ref(chunks), _indices[r+1:prev_r])...)
                        prev_r = r
                    end
                    push!(merged_chunks, _chunk)
                    _indices[l] = length(merged_chunks)
                    l += 1
                elseif _lengths[l] + _lengths[r] <= chunksize # merge
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

merge_chunks(sink, chunks...) = sink(TableOperations.joinpartitions(Tables.partitioner(identity, chunks)))

rowcount(chunk) = length(Tables.rows(chunk))

