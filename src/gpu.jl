import Base: serialize, deserialize, convert
@require ArrayFire begin 

    export GPUProc

    using ArrayFire
    immutable GPUProc <: Processor
        pid::Int
        #gpuid::Int -- todo: interface with many GPUs
    end
    _move(ctx, to_proc::GPUProc, x::AbstractPart) = AFArray(gather(ctx, x))
    function async_apply(ctx, p::GPUProc, thunk_id, f, data, chan, send_res)
        # for now, use a dedicated process to talk to GPUs
        remotecall(do_task, p.pid, ctx, p, thunk_id, f, data, chan, send_res)
    end

    function gather{H<:DistMem, T<:AFArray}(ctx, part::Part{H, T})
        fetch(Array, part.handle.ref)
    end

#   Base.convert{T,N}(::Type{Array{T,N}}, a::AFArray{T, N}) = Array(a)
end

