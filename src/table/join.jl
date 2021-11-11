import DataAPI: leftjoin, innerjoin

function leftjoin(d1::DTable, d2; on=nothing)
    f = (_d1, _d2, _on) -> leftjoin(_d1, _d2, on=_on)
    v = [Dagger.@spawn f(c, d2, on) for c in d1.chunks]
    DTable(v, d1.tabletype)
end


function innerjoin(d1::DTable, d2; on=nothing)
    f = (_d1, _d2, _on) -> innerjoin(_d1, _d2, on=_on)
    v = [Dagger.@spawn f(c, d2, on) for c in d1.chunks]
    DTable(v, d1.tabletype)
end


function _resolve_indices(d1, d2, on)
    isnothing(on) && error("yeet")
    if on isa Symbol
        l_cols = [on]
        r_cols = [on]
    elseif on isa Vector{Symbol}
        l_cols = on
        r_cols = on
    elseif on isa Vector{Pair{Symbol,Symbol}}
        l_cols = getindex.(on, 1)
        r_cols = getindex.(on, 2)
    end
    d1_names = collect(Tables.schema(Tables.columns(d1)).names)
    d2_names = collect(Tables.schema(Tables.columns(d2)).names)
    _l = length(l_cols)
    l_cols_indices = NTuple{_l,Int}(indexin(l_cols, d1_names))
    r_cols_indices = NTuple{_l,Int}(indexin(r_cols, d2_names))
    l_cols_indices, r_cols_indices
end


function leftjoin(l, r; on=nothing)
    l_ind, r_ind = _resolve_indices(l, r, on)
    _a = setdiff(1:length(Tables.columnnames(r)), r_ind)
    r_new_ind = NTuple{length(_a),Int}(_a)
    vl, vr = inner_indices(l, r, l_ind, r_ind, r_new_ind)
    vl2 = left_unmatched(l, vl)
    build_result_table_based_on_indices(l, r, vl, vr, vl2, l_ind, r_ind, r_new_ind)
end


function innerjoin(l, r; on=nothing)
    l_ind, r_ind = _resolve_indices(l, r, on)
    _a = setdiff(1:length(Tables.columnnames(r)), r_ind)
    r_new_ind = NTuple{length(_a),Int}(_a)
    vl, vr = inner_indices(l, r, l_ind, r_ind, r_new_ind)
    vl2 = [] 
    build_result_table_based_on_indices(l, r, vl, vr, vl2, l_ind, r_ind, r_new_ind)
end

function inner_indices(l, r, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1,N2}
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
    llenght = length(Tables.rows(l))
    filter(x -> x ∉ s, 1:llenght)
end

function build_result_table_based_on_indices(l, r, vl, vr, vl2, l_ind::NTuple{N1,Int}, r_ind::NTuple{N1,Int}, r_new_ind::NTuple{N2,Int}) where {N1,N2}
    fulllength = length(vl) + length(vl2)
    rcolnames = Tables.columnnames(Tables.columns(r))
    lcolnames = Tables.columnnames(Tables.columns(l))
    rrcolnames = getindex.(Ref(rcolnames), (2,))
    allcolnames = vcat(lcolnames..., rrcolnames...)


    cols = Vector{AbstractVector}(undef, N1 + N2)

    colcounter = 1
    for c in Tables.columns(l)
        newc = Vector{eltype(c)}(undef, fulllength)
        copyto!(newc, view(c, vl))
        copyto!(newc, length(vl) + 1, view(c, vl2))
        cols[colcounter] = newc
        colcounter += 1
    end
    for (i, c) in enumerate(Tables.columns(r))
        i ∉ r_new_ind && continue
        newc = Vector{Union{eltype(c),Missing}}(missing, fulllength)
        copyto!(newc, view(c, vr))
        cols[colcounter] = newc
        colcounter += 1
    end
    sink = Tables.materializer(l)
    sink((;zip(allcolnames, cols)...))
end

@inline function compare_rows(o, i, l_ind::NTuple{N,Int}, r_ind::NTuple{N,Int}) where {N}
    test = true
    @inbounds for x in 1:N
        test &= Tables.getcolumn(o, l_ind[x]) == Tables.getcolumn(i, r_ind[x])
        test || break
    end
    test
end

