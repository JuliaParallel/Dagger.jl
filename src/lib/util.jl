import Base: convert, +, *, /, -

###### Filesize algebra ######

export B, kB, MB, GB, TB

immutable Bytes
    val::Float64
end

(*)(n::Number, b::Bytes) = Bytes(n*b.val)
(+)(a::Bytes, b::Bytes) = Bytes(a.val+b.val)
(-)(a::Bytes, b::Bytes) = Bytes(a.val-b.val)
isless(a::Bytes, b::Bytes) = a.val < b.val
(/)(a::Bytes, b::Number) = Bytes(ceil(UInt64, a.val/b))
(/)(a::Bytes, b::Bytes) = a.val/b.val

const B = Bytes(1)
const kB = 1024B
const MB = 1024kB
const GB = 1024MB
const TB = 1024GB

convert(::Type{Bytes}, n::Integer) = Bytes(n)


##### Base patches ######

if VERSION < v"0.5.0-dev"
    const RemoteChannel = RemoteRef
end

import Base.intersect
# Add some niceties to Base.intersect

intersect(::Colon, ::Colon) = Colon()
intersect(::Colon, r) = r
intersect(r, ::Colon) = r


##### Utility functions ######

"""
Utility function to divide the range `range` into `n` chunks
"""
function split_range{T}(range::Range{T}, n)
    len = length(range)

    starts = len >= n ?
        round(T, linspace(first(range), last(range)+1, n+1)) :
        [[first(range):(last(range)+1);], zeros(T, n-len);]

    map((x,y)->x:y, starts[1:end-1], starts[2:end] .- 1)
end

function split_range(r::Range{Char}, n)
    map((x) -> Char(first(x)):Char(last(x)), split_range(Int(first(r)):Int(last(r)), n))
end

"""
    split_range_interval(range, n)
split a range into pieces each of length `n` or lesser
"""
function split_range_interval(range, n)
    f = first(range)
    N = length(range)
    npieces = ceil(Int, N / n)

    ranges = Array(UnitRange, npieces)
    for i=1:npieces
        ranges[i] = f:min(N, f+n-1)
        f += n
    end
    ranges
end


###### interface macro ######

getvarname(x::Expr) = x.args[1]
getvarname(x::Symbol) = x

"""
    @unimplemented fname(<args...>)

While it is nice to define generic function ad-hoc, it can sometimes
get confusing to figure out which method is missing.
`@interface` creates a function which errors out pointing
which method is missing.
"""
macro unimplemented(expr)
    @assert expr.head == :call

    fname = expr.args[1]
    args = expr.args[2:end]
    sig = string(expr)

    vars = map(x->getvarname(x), args)
    typs = Expr(:vect, map(x -> :(typeof($x)), vars)...)


    :(function $(esc(fname))($(args...))
        error(string("a method of ", $sig, " specialized to ", ($typs...), " is missing"))
    end)
end

"""
Tree reduce
"""
function treereduce(f, xs)
    length(xs) == 1 && return xs[1]
    l = length(xs)
    m = div(l, 2)
    f(treereduce(f, xs[1:m]), treereduce(f, xs[m+1:end]))
end

const  ENABLE_DEBUG = true # Will remove code under @dbg

"""
Run a block of code only if DEBUG is true
"""
macro dbg(expr)
    if ENABLE_DEBUG
        esc(expr)
    end
end

function treereducedim(op, xs::Array, dim::Int)
    l = size(xs, dim)
    colons = Any[Colon() for i=1:length(size(xs))]
    if dim > length(size(xs))
        return xs
    end
    ys = treereduce((x,y)->map(op, x,y), Any[begin
        colons[dim] = [i]
        @compat view(xs, colons...)
    end for i=1:l])
    reshape(ys[:], Base.reduced_dims(size(xs), dim))
end

function treereducedim(op, xs::Array, dim::Tuple)
    reduce((prev, d) -> treereducedim(op, prev, d), xs, dim)
end

function setindex{N}(x::NTuple{N}, idx, v)
    map(ifelse, ntuple(x->is(idx, x), Val{N}), ntuple(x->v, Val{N}), x)
end
