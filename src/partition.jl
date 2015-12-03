
"""
A partition pattern. Implements `slice` and `gather` methods
"""
abstract AbstractPartition

immutable CutDim{d} <: AbstractPartition end
cutdim(n) = CutDim{n}()

function slice{d}(arr::AbstractArray, ::CutDim{d}, devs)
    # TODO
end
function gather{d}(ctx, partition::CutDim{d}, xs::Vector)
    reduce((acc, x) -> cat(d, acc, x), xs[1], xs[2:end])
end


immutable Bcast <: AbstractPartition end

function slice{T}(x::T, ::Bcast, targets)
    T[x for i in 1:length(targets)]
end
