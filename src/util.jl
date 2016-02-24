

###### Filesize algebra ######

export B, kB, MB, GB, TB

immutable Bytes
    val::Float64
end

Base.(:*)(n::Number, b::Bytes) = Bytes(n*b.val)
Base.(:+)(a::Bytes, b::Bytes) = Bytes(a.val+b.val)
Base.(:-)(a::Bytes, b::Bytes) = Bytes(a.val-b.val)
Base.isless(a::Bytes, b::Bytes) = a.val < b.val
Base.(:/)(a::Bytes, b::Number) = Bytes(ceil(UInt64, a.val/b))
Base.(:/)(a::Bytes, b::Bytes) = a.val/b.val

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
Utility function to divide the range `range` into `n` parts
"""
function split_range(range, n)
    len = length(range)

    starts = len >= n ?
        round(Int, linspace(first(range), last(range)+1, n+1)) :
        [[first(range):(last(range)+1);], zeros(Int, n-len);]

    map(UnitRange, starts[1:end-1], starts[2:end] .- 1)
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
    @interface fname(<args...>)

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
