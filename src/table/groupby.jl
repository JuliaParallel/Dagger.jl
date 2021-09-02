
function groupby(d::DTable, col; merge=true, chunksize=0)
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

    build_index = (merge, chunksize, vs...) -> begin
        v = vcat(vs...)
        ks = unique(map(x-> x[1], v))
        chunks = Vector{Union{EagerThunk, Nothing}}(map(x-> x[2], v))
        
        idx = Dict([k => Vector{Int}() for k in ks])
        for (i, k) in enumerate(map(x-> x[1], v))
            push!(idx[k], i) 
        end
        
        if merge && chunksize <= 0 # merge all partitions into one
            sink = Tables.materializer(tabletype(d)())
            v2 = Vector{EagerThunk}()
            sizehint!(v2, length(keys(idx)))
            for (i, k) in enumerate(keys(idx))
                c = getindex.(Ref(chunks), idx[k])
                push!(v2, Dagger.@spawn merge_chunks(sink, c...))
                idx[k] = [i]
            end
            idx, v2
        elseif merge && chunksize > 0 # merge all but keep the chunking approximately at chunksize with minimal merges
            sink = Tables.materializer(tabletype(d)())
            for (i, k) in enumerate(keys(idx))
                _indices = idx[k]
                _chunks = getindex.(Ref(chunks), _indices)
                _lengths = fetch.(Dagger.spawn.(rowcount, _chunks))
                c = collect.(collect(zip(_indices, _chunks, _lengths)))
                sort!(c, by=(x->x[3]), rev=true)

                l = 1
                r = length(c)
                prev_r = r
                while l < r
                    if c[l][3] >= chunksize
                        if r < prev_r
                            c[l][2] = Dagger.@spawn merge_chunks(sink, c[l][2], getindex.(c[r+1:prev_r], 2)...)
                            prev_r = r
                        end
                        l += 1
                    elseif c[l][3] + c[r][3] > chunksize
                        if r < prev_r
                            c[l][2] = Dagger.@spawn merge_chunks(sink, c[l][2], getindex.(c[r+1:prev_r], 2)...)
                            prev_r = r
                        end
                        l += 1
                        
                    elseif c[l][3] + c[r][3] <= chunksize # merge
                        c[l][3] = c[l][3] + c[r][3]
                        r -= 1
                    end
                end
                @assert l == r
                for i in 1:length(c)
                    if i <= l
                        chunks[c[i][1]] = c[i][2]
                    else
                        chunks[c[i][1]] = nothing
                    end
                end
                idx[k] = map(x-> x[1], c[1:l])
            end
            idx, filter(x-> !isnothing(x), chunks)
        else
            idx, chunks
        end
    end

    res = Dagger.@spawn build_index(merge, chunksize, v...)
    r = fetch(res)
    DTable(VTYPE(r[2]), d.tabletype, Dict(col => r[1])) 
end

merge_chunks(sink, chunks...) = sink(TableOperations.joinpartitions(Tables.partitioner(x -> x, chunks)))

rowcount(chunk) = length(Tables.rows(chunk))

