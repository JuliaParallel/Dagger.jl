
import Base: transpose

immutable Transpose <: Computation
    input::Computation
end

global _stage_cache = WeakKeyDict()

function cached_stage(ctx, x)
    isimmutable(x) && return stage(ctx, x)
    if haskey(_stage_cache, x)
        _stage_cache[x]
    else
        _stage_cache[x] = stage(ctx, x)
    end
end

transpose(x::AbstractPart) = Thunk(transpose, (x,))
transpose(x::Computation) = Transpose(x)
function transpose(x::DenseDomain{2})
    d = indexes(x)
    DenseDomain(d[2], d[1])
end

function stage(ctx, node::Cat)
    node
end

function stage(ctx, node::Transpose)
    inp = cached_stage(ctx, node.input)
    dmn = domain(inp)
    @assert isa(dmn, DomainBranch)
    dmnT = DomainBranch(head(dmn)', dmn.children')
    Cat(inp.partition, parttype(inp), dmnT, inp.parts')
end


import Base: *, +

immutable MatMul <: Computation
    a::Computation
    b::Computation
end

(*)(a::Computation, b::Computation) = MatMul(a,b)

function (*)(a::ArrayDomain{2}, b::ArrayDomain{2})

    if size(a, 2) != size(b, 1)
        DimensionMismatch("The domains cannot be multiplied")
    end

    DenseDomain((indexes(a)[1], indexes(b)[2]))
end
function (*)(a::ArrayDomain{2}, b::ArrayDomain{1})
    if size(a, 2) != length(b)
        DimensionMismatch("The domains cannot be multiplied")
    end
    b
end

function (*){D1<:ArrayDomain, D2<:ArrayDomain}(
        a::DomainBranch{D1}, b::DomainBranch{D2}
    )
    DomainBranch(head(a)*head(b), _mul(a.children, b.children))
end

function (*)(a::BlockPartition{2}, b::BlockPartition{2})
    BlockPartition(a.blocksize[1], b.blocksize[2])
end
(*)(a::BlockPartition{2}, b::BlockPartition{1}) = b

function (+)(a::ArrayDomain, b::ArrayDomain)
    if a == b
        DimensionMismatch("The domains cannot be added")
    end
    a
end

(*)(a::Thunk, b::Thunk) = Thunk(*, (a,b))
(+)(a::Thunk, b::Thunk) = Thunk(+, (a,b))

# we define our own matmat and matvec multiply
# for computing the new domains and thunks.
function _mul(a::Matrix, b::Matrix)
    c = Array(eltype(a), (size(a,1), size(b,2)))
    for i=1:size(a,1)
        for j=1:size(b, 2)
            c[i,j] = reduce(+, map(*, a[i,:], b[:, j]))
        end
    end
    c
end

function _mul(a::Matrix, b::Vector)
    c = Array(eltype(b), size(a,1))
    for i=1:size(a,1)
        c[i] = reduce(+, map(*, a[i, :], b))
    end
    c
end

function stage(ctx, mul::MatMul)
    a = stage(ctx, mul.a)
    b = stage(ctx, mul.b)

    pa = partition(a)::BlockPartition
    pb = partition(b)::BlockPartition
    p = pa*pb

    da = domain(a)
    db = domain(b)

    d = da*db
    Cat(p, Any, d, _mul(a.parts, b.parts))
end
