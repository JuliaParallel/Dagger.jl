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
    v = [Dagger.@spawn newjoinflow(c, d2, l_cols_indices, r_cols_indices,r_new_ind) for c in d1.chunks]
    # v = [Dagger.@spawn _leftjoin_nonunique_r(c, d2, l_cols_indices, r_cols_indices,r_new_ind) for c in d1.chunks]
    DTable(v, d1.tabletype)
end



# per partition, full r table joining
function _leftjoin(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    sink = Tables.materializer(l)
    collectors = Vector{Vector}()
    sizehint!(collectors, length(r_new_ind))

    rschema = Tables.schema(Tables.rows(r))
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
            if cmp
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

function _leftjoin_nonunique_r(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    sink = Tables.materializer(l)
    collectors = Vector{Vector}()
    sizehint!(collectors, length(r_new_ind))

    rschema = Tables.schema(Tables.rows(r))
    rnames = rschema.names
    llength = length(Tables.rows(l))

    repeat_previous = BitVector()
    sizehint!(repeat_previous, llength)

    @inbounds for i in r_new_ind
        t = isnothing(rschema.types) ? Any : rschema.types[i]
        p = Vector{Union{t, Missing}}()
        sizehint!(p, llength)
        push!(collectors, p)
    end

    @inbounds for o in Tables.rows(l)
        cmp = false
        already_matched = false

        for i in Tables.rows(r)
            cmp = compare_rows(o, i, l_ind, r_ind)
            if cmp
                for j in 1:N2
                    push!(collectors[j], Tables.getcolumn(i, r_new_ind[j]))
                end
                if already_matched
                    push!(repeat_previous, true)
                else
                    push!(repeat_previous, false)
                    already_matched = true
                end
            end
        end
        if !already_matched
            push!(repeat_previous, false)
            @inbounds for j in 1:N2
                push!(collectors[j], missing)
            end
        end
    end

    function expand_column(col, repeat_previous)
        newcol = Vector{eltype(col)}()
        sizehint!(newcol, length(repeat_previous))
        ind = 0
        for b in repeat_previous
            if !b ind += 1 end
            push!(newcol, col[ind])
        end
        newcol
    end

    left_cols = Tables.columntable(l)
    new_cols = [rnames[col_idx] => collectors[i] for (i, col_idx) in enumerate(r_new_ind)]
    sink((;[k => expand_column(left_cols[k], repeat_previous) for k in Tables.columnnames(left_cols)]..., new_cols...))
end

function newjoinflow(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    vl, vr = inner_indices(l,r,l_ind, r_ind, r_new_ind)
    vl2 = left_unmatched(l, vl)
    build_result_table_based_on_indices(l,r, vl, vr, vl2, l_ind, r_ind, r_new_ind)
end

function inner_indices(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    
    llength = length(Tables.rows(l))
    vl = Vector{Int}()
    sizehint!(vl, llength)
    vr = Vector{Int}()
    sizehint!(vr, llength)
    for (oind, oel) in enumerate(Tables.rows(l))
        for (iind, iel) in enumerate(Tables.rows(r))
            cmp = compare_rows(oel, iel, l_ind, r_ind)
            if cmp
                push!(vl, oind)
                push!(vr, iind)
            end
        end
    end
    vl, vr
end

function left_unmatched(l, vl)
    s = Set(vl)
    llenght=length(Tables.rows(l))
    filter(x->x∉s, 1:llenght)
end

function build_result_table_based_on_indices(l, r, vl, vr, vl2, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1, N2}
    fulllength = length(vl) + length(vl2)
    rcolnames = Tables.columnnames(Tables.columns(r))
    lcolnames = Tables.columnnames(Tables.columns(l))
    rrcolnames = getindex.(Ref(rcolnames), (2,))
    allcolnames = vcat(lcolnames..., rrcolnames...)


    cols = Vector{AbstractVector}(undef, N1+N2)

    colcounter = 1
    for c in Tables.columns(l)
        newc = Vector{eltype(c)}(undef, fulllength)
        copyto!(newc, view(c, vl))
        copyto!(newc, length(vl) + 1, view(c, vl2))
        cols[colcounter] = newc
        colcounter += 1
    end
    for (i,c) in enumerate(Tables.columns(r))
        i ∉ r_new_ind && continue
        newc = Vector{Union{eltype(c), Missing}}(missing, fulllength)
        copyto!(newc, view(c, vr))
        cols[colcounter] = newc
        colcounter += 1
    end
    sink = Tables.materializer(l)
    # this sink allocates, but not sure if that's the best idea, because the columns are already copied at this point
    sink([key => col for (key, col) in zip(allcolnames, cols)])
end

function compare_rows(o, i, l_ind::NTuple{N, Int}, r_ind::NTuple{N, Int}) where {N}
    test = true
    @inbounds for x in 1:N
        test &= Tables.getcolumn(o, l_ind[x]) == Tables.getcolumn(i, r_ind[x])
        test || break
    end
    test
end

