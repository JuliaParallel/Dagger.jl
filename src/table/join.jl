

function leftjoin(d1, d2)
    sink = Tables.materializer(d1)
    cols = [:id]
    rowmap = row -> (Tables.getcolumn(row, :id),)
    
    bvec = Vector{Union{Int, Missing}}()
    schema = Tables.schema(d2)
    
    names = filter(x -> x ∉ cols, schema.names)
    collectors = []
    for n in names
        push!(collectors, Vector{Union{Any, Missing}}())
    end

    for o in Tables.rows(d1)
        ro = rowmap(o)
        ri = missing
        for i in Tables.rows(d2)
            if ro == rowmap(i)
                ri = i
                break
            end
        end
        for (idx,n) in enumerate(names)
            push!(collectors[idx], ri === missing ? missing : Tables.getcolumn(ri, n))
        end
    end
    sink((;Tables.columntable(d1)..., [n => collectors[idx] for (idx, n) in enumerate(names)]...))
end