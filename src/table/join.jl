function _resolve_indices(d1, d2, on)
    isnothing(on) && error("yeet")
    if on isa Symbol
        l_cols = [on]
        r_cols = [on]
    elseif on isa Vector{Symbol}
        l_cols = on
        r_cols = on
    elseif on isa Vector{Pair{Symbol, Symbol}}
        l_cols = getindex.(on, 1)
        r_cols = getindex.(on, 2)
    end
    d1_names = collect(Tables.schema(d1).names)
    d2_names = collect(Tables.schema(d2).names)
    _l = length(l_cols)
    l_cols_indices = NTuple{_l, Int}(indexin(l_cols, d1_names))
    r_cols_indices = NTuple{_l, Int}(indexin(r_cols, d2_names))
    l_cols_indices, r_cols_indices
end

function leftjoin(d1::DTable, d2; on=nothing, d2_mode=:full)
    l_cols_indices, r_cols_indices = _resolve_indices(d1, d2, on)
    _a = setdiff(1:length(Tables.columnnames(d2)), r_cols_indices)
    r_new_ind = NTuple{length(_a), Int}(_a)
    v = [Dagger.@spawn _leftjoin(c, d2, l_cols_indices, r_cols_indices,r_new_ind) for c in d1.chunks]
    DTable(v, d1.tabletype)
end



# per partition, full r table joining
function _leftjoin(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    sink = Tables.materializer(l)
    collectors = Vector{Vector}()
    sizehint!(collectors, length(r_new_ind))

    rschema = Tables.schema(r)
    rnames = rschema.names
    llength = length(Tables.rows(l))

    @inbounds for i in r_new_ind
        t = isnothing(rschema.types) ? Any : rschema.types[i]
        p = Vector{Union{t, Missing}}()
        sizehint!(p, llength)
        push!(collectors, p)
    end

    @inbounds for o in Tables.rows(l)
        cmp = false
        for i in Tables.rows(r)
            cmp = compare_rows(o, i, l_ind, r_ind)
            if test
                for j in 1:N2
                    push!(collectors[j], Tables.getcolumn(i, r_new_ind[j]))
                end
                break
            end
        end
        if !cmp
            @inbounds for j in 1:N2
                push!(collectors[j], missing)
            end
        end
    end

    left_cols = Tables.columntable(l)
    new_cols = [rnames[col_idx] => collectors[i] for (i, col_idx) in enumerate(r_new_ind)]
    sink((;left_cols..., new_cols...))
end

function compare_rows(o, i, l_ind::NTuple{N, Int}, r_ind::NTuple{N, Int}) where {N}
    test = true
    @inbounds for x in 1:N
        test &= Tables.getcolumn(o, l_ind[x]) == Tables.getcolumn(i, r_ind[x])
        test || break
    end
    test
end
