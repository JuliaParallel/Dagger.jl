export Accumulator, accumulate!

### Accumulator ###

const _accumulators = Dict{UInt64, Any}()
const _proc_accumulators = Dict{UInt64, Any}()

immutable Accumulator{F, T}
    id::UInt64
    operation::F
    zero::T
    function Accumulator(id, op::F, zero::T)
        acc = new(id, op, zero)
        _accumulators[id] = (acc, zero)
        acc
    end
end
Accumulator{F,T}(f::F, x::T) = Accumulator{F,T}(rand(UInt64), f, x)
Base.get(acc::Accumulator) = get(_accumulators, acc.id, (acc, acc.zero))[2]

function accumulate!(acc::Accumulator, val)
    # "add" val to the accumulated value in
    # task local storage

    _proc_accumulators[acc.id] = (acc, acc.operation(get(_proc_accumulators, acc.id, (nothing, acc.zero))[2], val))
end

