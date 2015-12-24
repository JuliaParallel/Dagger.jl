export Accumulator, accumulate!, release

### Accumulator ###

_accid = UInt64(0)
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
Accumulator{F,T}(f::F, x::T) = Accumulator{F,T}(next_acc_id(), f, x)
Base.get(acc::Accumulator) = get(_accumulators, acc.id, (acc, acc.zero))[2]

function next_acc_id()
    global _accid
    _accid += UInt64(1)
end

function accumulate!(acc::Accumulator, val)
    # "add" val to the accumulated value in
    # task local storage

    _proc_accumulators[acc.id] = (acc, acc.operation(get(_proc_accumulators, acc.id, (nothing, acc.zero))[2], val))
end

function release(acc::Accumulator)
    delete!(ComputeFramework._accumulators, acc.id)
    for pid in workers()
        remotecall_fetch((id)->(delete!(ComputeFramework._proc_accumulators, id); nothing), pid, acc.id)
    end
end
