import DataAPI: leftjoin, innerjoin


function leftjoin(d1::DTable, d2; on = nothing)
    f = (_d1, _d2, _on) -> leftjoin(_d1, _d2, on = _on)
    v = [Dagger.@spawn f(c, d2, on) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function leftjoin(l, r; on = nothing)
    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)
    inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r)
    outer_l = find_outer_indices(l, inner_l)
    build_joined_table(:leftjoin, names, l, r, inner_l, inner_r, outer_l, other_r)
end


function innerjoin(d1::DTable, d2; on = nothing)
    f = (_d1, _d2, _on) -> innerjoin(_d1, _d2, on = _on)
    v = [Dagger.@spawn f(c, d2, on) for c in d1.chunks]
    DTable(v, d1.tabletype)
end

function innerjoin(l, r; on = nothing)
    names, _, other_r, cmp_l, cmp_r = resolve_colnames(l, r, on)
    inner_l, inner_r = match_inner_indices(l, r, cmp_l, cmp_r)
    outer_l = Set{UInt}()
    build_joined_table(:innerjoin, names, l, r, inner_l, inner_r, outer_l, other_r)
end
