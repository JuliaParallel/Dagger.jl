TiledMatrix = Union{Matrix{Float64}}

if !isdefined(Main, :TileMat)
mutable struct TileMat
    m::Int64
    n::Int64
    mb::Int64
    nb::Int64
    mt::Int64
    nt::Int64
    mat::Array{TiledMatrix, 2}
    #mat::Array{Dagger.Chunk, 2}
    function TileMat(m::Int64, n::Int64, nb::Int64, mb::Int64)
        mt = (mod(m,mb)==0) ? floor(Int64, (m / mb)) : floor(Int64, (m / mb) + 1) 
        nt = (mod(n,nb)==0) ? floor(Int64,(n / nb)) : floor(Int64, (n / nb) + 1)
        mat = Array{TiledMatrix, 2}(undef, mt, nt)
        new(m, n, nb, mb, mt, nt, mat)
    end
end
end
# This TileWorkSpace is used for RH QR to store the reduction T 
if !isdefined(Main, :TileWorkSpace)
    mutable struct TileWorkSpace
        m::Int64
        n::Int64
        ib::Int64
        nb::Int64
        mt::Int64
        nt::Int64
        mat::Array{TiledMatrix, 2}
        #mat::Array{Dagger.Chunk, 2}
        function TileWorkSpace(m::Int64, n::Int64, ib::Int64, nb::Int64)
            MT = (mod(m,nb)==0) ? floor(Int64, (m / nb)) : floor(Int64, (m / nb) + 1) 
            NT = ((mod(n,nb)==0) ? floor(Int64,(n / nb)) : floor(Int64, (n / nb) + 1) * 2)
            lm = ib * MT;
            ln = nb * NT;
            mt = (mod(lm,ib)==0) ? floor(Int64, (lm / ib)) : floor(Int64, (lm / ib) + 1) 
            nt = ((mod(ln,nb)==0) ? floor(Int64,(ln / nb)) : floor(Int64, (ln / nb) + 1))
            mat = Array{TiledMatrix, 2}(undef, mt, nt)
            new(lm, ln, ib, nb, mt, nt, mat)
        end
    end
end

function generate_tiles!(::Type{T}, tilemat::TileMat) where {T <: Number}

    Threads.@threads for i in range(1, tilemat.mt)
        num_rows = i == tilemat.mt ? tilemat.m-(i-1)*tilemat.mb : tilemat.mb
        for j in range(1, tilemat.nt)
            num_cols = j == tilemat.nt ? tilemat.n-(j-1)*tilemat.nb : tilemat.nb
            tilemat.mat[i, j] = rand(T, num_rows, num_cols)
        end
    end
end

function init_workspace!(::Type{T}, workspacemat::TileWorkSpace) where {T <: Number}

    Threads.@threads for i in range(1, workspacemat.mt)
        num_rows = i == workspacemat.mt ? workspacemat.m-(i-1)*workspacemat.ib : workspacemat.ib
        for j in range(1, workspacemat.nt)
            num_cols = j == workspacemat.nt ? workspacemat.n-(j-1)*workspacemat.nb : workspacemat.nb
            workspacemat.mat[i, j] = zeros(T, num_rows, num_cols)
        end
    end
end

function tile2lap!(tilemat::TileMat, dense::Matrix{Float64})
    Am = size(dense, 1)
    An = size(dense, 2)
    nb = tilemat.nb
    
    m = tilemat.m
    n = tilemat.n
    
    @assert Am == m
    @assert An == n
    mm = nb
    nn = nb
    mt = tilemat.mt
    nt = tilemat.nt

    for j in range(1, nt)
        num_cols = j == tilemat.nt ? tilemat.n-(j-1)*tilemat.nb : tilemat.nb
        for i in range(1, mt)
            num_rows = i == tilemat.mt ? tilemat.m-(i-1)*tilemat.mb : tilemat.mb
            starti = 1 + (i - 1) * nb
            startj = 1 + (j - 1) * nb
            dense[starti:starti+num_rows-1, startj:startj+num_cols-1] = tilemat.mat[i, j] 
        end
    end
end


function tile2lap!(tilemat::TileWorkSpace, dense::Matrix{Float64})
    Am = size(dense, 1)
    An = size(dense, 2)
    nb = tilemat.nb
    
    m = tilemat.m
    n = tilemat.n
    
    @assert Am == m
    @assert An == n
    mm = nb
    nn = nb
    mt = tilemat.mt
    nt = tilemat.nt

    for j in range(1, nt)
        num_cols = j == tilemat.nt ? tilemat.n-(j-1)*tilemat.nb : tilemat.nb
        for i in range(1, mt)
            num_rows = i == tilemat.mt ? tilemat.m-(i-1)*tilemat.ib : tilemat.ib
            starti = 1 + (i - 1) * nb
            startj = 1 + (j - 1) * nb
            dense[starti:starti+num_rows-1, startj:startj+num_cols-1] = tilemat.mat[i, j] 
        end
    end
end

function diagposv!(::Type{T}, tilemat::TileMat, val) where {T<: Number}
    mt = tilemat.mt
    nt = tilemat.nt
    mb = tilemat.mb

    for i in range(1, mt)
        num_rows = i == tilemat.mt ? tilemat.m-(i-1)*tilemat.mb : tilemat.mb
        for m in range(1, num_rows) 
            tilemat.mat[i,i][(m-1) * num_rows + m] += val
        end
    end
end 


function tilecheck(tilemat::TileMat, dense::Matrix{Float64})
    Am = size(dense, 1)
    An = size(dense, 2)
    nb = tilemat.nb
    m = tilemat.m
    n = tilemat.n
    @assert Am == m
    @assert An == n
    mm = nb
    nn = nb
    mt = tilemat.mt
    nt = tilemat.nt
    for j in range(1, nt)
        num_cols = j == tilemat.nt ? tilemat.n-(j-1)*tilemat.nb : tilemat.nb
        for i in range(1, mt)
            num_rows = i == tilemat.mt ? tilemat.m-(i-1)*tilemat.mb : tilemat.mb
            starti = 1 + (i - 1) * nb
            startj = 1 + (j - 1) * nb
            R = norm(dense[starti:starti+num_rows-1, startj:startj+num_cols-1] - tilemat.mat[i, j], 2)
            id = Base.unsafe_trunc(UInt8, objectid(tilemat.mat[i, j]))
            if R > 1e-10
                @warn "Norm of [$id] ($i, $j): $R"
            end
        end
    end
end