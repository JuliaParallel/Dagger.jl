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


function _resolve_indices(l, r, on)
    isnothing(on) && error("yeet")
    if on isa Symbol
        l_cmp_symbols = [on]
        r_cmp_symbols = [on]
    elseif on isa Vector{Symbol}
        l_cmp_symbols = on
        r_cmp_symbols = on
    elseif on isa Vector{Pair{Symbol,Symbol}}
        l_cmp_symbols = getindex.(on, 1)
        r_cmp_symbols = getindex.(on, 2)
    end

    l_cols = Tables.columns(l)
    l_schema = Tables.schema(l_cols)
    cmp_l_indices = Tables.columnindex.(Ref(l_schema), l_cmp_symbols)
    other_l_indices = setdiff(1:length(l_cols), cmp_l_indices)
    cmp_l = NTuple{length(cmp_l_indices), Int}(cmp_l_indices)
    other_l = NTuple{length(other_l_indices), Int}(other_l_indices)

    r_cols = Tables.columns(r)
    r_schema = Tables.schema(r_cols)
    cmp_r_indices = Tables.columnindex.(Ref(r_schema), r_cmp_symbols)
    other_r_indices = setdiff(1:length(r_cols), cmp_r_indices)
    cmp_r = NTuple{length(cmp_r_indices), Int}(cmp_r_indices)
    other_r = NTuple{length(other_r_indices), Int}(other_r_indices)

    rnames = (name in l_schema.names ? Symbol(string(name) * "_2") : name for name in getindex.(Ref(r_schema.names), other_r_indices))
    final_nameset = (l_schema.names..., rnames...)

    final_nameset, other_l, other_r, cmp_l, cmp_r
end


function leftjoin(l, r; on=nothing)
    names, other_l, other_r, cmp_l, cmp_r = _resolve_indices(l, r, on)
    inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r)
    outer_l = match_outer_left_indices(l, inner_l)
    build_result_table_based_on_indices(l, r, inner_l, inner_r, outer_l, other_l,  other_r, names)
end


function innerjoin(l, r; on=nothing)
    names, other_l, other_r, cmp_l, cmp_r = _resolve_indices(l, r, on)
    inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r)
    outer_l = Vector{Int}()
    build_result_table_based_on_indices(l, r, inner_l, inner_r, outer_l,other_l, other_r, names)
end

function match_inner_indices(l, r, l_ind::NTuple{N, Int}, r_ind::NTuple{N, Int}) where {N}
    l_length = length(Tables.rows(l))
    vl = Vector{UInt}()
    vr = Vector{UInt}()
    sizehint!(vl, l_length) # these vectors will be at least this long
    sizehint!(vr, l_length)
    for (oind, oel) in enumerate(Tables.rows(l))
        for (iind, iel) in enumerate(Tables.rows(r))
            if compare_rows(oel, iel, l_ind, r_ind)
                push!(vl, oind)
                push!(vr, iind)
            end
        end
    end
    vl, vr
end

function match_outer_left_indices(l, vl)
    s = Set(vl)
    llenght = length(Tables.rows(l))
    filter(x -> x ∉ s, 1:llenght)
end


function build_result_table_based_on_indices(l, r, vl, vr, vl2, other_l, r_new_ind, names)
    fulllength = length(vl) + length(vl2)
    allcolnames = names

    cols = Vector{AbstractVector}(undef, length(names))

    colcounter = 1
    for c in Tables.columns(l)
        newc = Vector{eltype(c)}(undef, fulllength)
        copyto!(newc, view(c, vl))
        copyto!(newc, length(vl) + 1, view(c, vl2))
        cols[colcounter] = newc
        colcounter += 1
    end

    rcols = Tables.columns(r)
    for i in r_new_ind
        c = rcols[i]
        newc = Vector{Union{eltype(c), Missing}}(missing, fulllength)
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

