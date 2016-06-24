# Create artist alias dict
# Download the dataset from:  http://bit.ly/1KiJdOR
using Dagger

function read_artist_map(file)
    t1 = time()
    A = readdlm(file, '\t')
    valid = map(x->isa(x, Integer), A)
    valid = valid[:,1] & valid[:,2]
    Avalid = convert(Matrix{Int64}, A[valid, :])

    amap = Dict{Int64,Int64}()
    for idx in 1:size(Avalid,1)
        bad_id = Avalid[idx, 1]
        good_id = Avalid[idx, 2]
        amap[bad_id] = good_id
    end
    println("read artist map in $(time()-t1) secs")
    amap
end

@everywhere function make_sparse(X, amap, m, n)

    for idx in 1:size(X,1)
        artist_id = X[idx,2]
        if artist_id in keys(amap)
            X[idx,2] = amap[artist_id]
        end
    end
    users   = convert(Vector{Int64},   X[:,1])
    artists = convert(Vector{Int64},   X[:,2])
    ratings = convert(Vector{Float64}, X[:,3])
    S = sparse(users, artists, ratings, m, n)
    # Keep only non-empty rows / columns
    nzrows = find(reducedim(+, S, 2) .!= 0.)
    nzcols = find(reducedim(+, S, 1) .!= 0.)
    S[nzrows, nzcols]
end

read_trainingset(file, dest, amap::AbstractString, parts) = read_trainingset(file, dest, read_artist_map(amap), parts)
function read_trainingset(file, dest, amap, parts)
    raw = readdlm(file, ' ')
    m = maximum(raw[:, 1])
    n = maximum(raw[:, 2])
    X = make_sparse(raw, amap,m,n)
    m1,n1 = size(X)
    # Column distributed
    save(Distribute(BlockPartition(Int(m1), ceil(Int, n1/parts)), X), "X_col") |> compute
    # And row distributed
    #save(Distribute(BlockPartition(ceil(Int, m1/parts), Int(n1)), X), "X_row") |> compute
end

read_trainingset("profiledata_06-May-2005/user_artist_data.txt", "X", "profiledata_06-May-2005/artist_alias.txt", 10)
