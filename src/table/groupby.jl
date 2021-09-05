function groupby(d::DTable, col::Symbol; merge=true, chunksize=0)
    distinct_values = (_chunk, _col) -> unique(Tables.getcolumn(_chunk, _col))

    filter_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end

    chunk_wrap = (_chunk, _col) -> begin
        vals = distinct_values(_chunk, _col)
        sort!(vals)
        if length(vals) > 1
            [v => Dagger.@spawn filter_wrap(_chunk, x -> Tables.getcolumn(x, _col) .== v) for v in vals]
        else
            [vals[1] => Dagger.@spawn (x->x)(_chunk)]
        end
    end

    v = [Dagger.@spawn chunk_wrap(c, col) for c in d.chunks]

    #ret = _build_groupby_index(merge, chunksize, tabletype(d), fetch.(v)...)
    ret = fetch(Dagger.@spawn _build_groupby_index(merge, chunksize, tabletype(d), v...))
    DTable(VTYPE(ret[2]), d.tabletype, Dict(col => ret[1])) 
end

function _build_groupby_index(merge::Bool, chunksize::Int, tabletype, vs...)
    v = vcat(vs...)
    ks = unique(map(x-> x[1], v))
    chunks = Vector{Union{EagerThunk, Nothing}}(map(x-> x[2], v))

    idx = Dict([k => Vector{Int}() for k in ks])
    for (i, k) in enumerate(map(x-> x[1], v))
        push!(idx[k], i) 
    end

    if merge && chunksize <= 0 # merge all partitions into one
        sink = Tables.materializer(tabletype())
        v2 = Vector{EagerThunk}()
        sizehint!(v2, length(keys(idx)))
        for (i, k) in enumerate(keys(idx))
            c = getindex.(Ref(chunks), idx[k])
            push!(v2, Dagger.@spawn merge_chunks(sink, c...))
            idx[k] = [i]
        end
        idx, v2
    elseif merge && chunksize > 0 # merge all but try to merge all the small chunks into chunks of chunksize
        sink = Tables.materializer(tabletype())
        for k in keys(idx)
            _indices = idx[k]
            _chunks = getindex.(Ref(chunks), _indices)
            _lengths = fetch.(Dagger.spawn.(rowcount, _chunks))
            
            c = collect.(collect(zip(_indices, _lengths, _chunks)))
            index = 1; len = 2; chunk = 3

            sort!(c, by=(x->x[len]), rev=true)

            l = 1
            r = length(c)
            prev_r = r

            while l <= r
                if c[l][len] >= chunksize || c[l][len] + c[r][len] > chunksize || l == r
                    if r < prev_r
                        c[l][chunk] = Dagger.@spawn merge_chunks(sink, c[l][chunk], getindex.(c[r+1:prev_r], chunk)...)
                        prev_r = r
                    end
                    l += 1
                elseif c[l][len] + c[r][len] <= chunksize # merge
                    c[l][len] += c[r][len]
                    r -= 1
                end
            end
            for i in 1:length(c)
                chunks[c[i][index]] = i <= r ? c[i][chunk] : nothing
            end
            idx[k] = map(x-> x[index], c[1:r])
        end
        idx, filter(x-> x !== nothing, chunks)
    else
        idx, chunks
    end
end

merge_chunks(sink, chunks...) = sink(TableOperations.joinpartitions(Tables.partitioner(x -> x, chunks)))

rowcount(chunk) = length(Tables.rows(chunk))

