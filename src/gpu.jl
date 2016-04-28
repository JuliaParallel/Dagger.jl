import Base: serialize, deserialize
@require ArrayFire begin 
    immutable AFWrap
    val::Any
    end

    export GPUProc

    using ArrayFire
    immutable GPUProc <: Processor
        pid::Int
        #gpuid::Int -- todo: interface with many GPUs
    end
    unwrap_af(x::AFWrap) = x.val
    unwrap_af(x) = x
    _move(ctx, to_proc::GPUProc, x::AbstractPart) = AFWrap(AFArray(gather(ctx, x)))
    function async_apply(ctx, p::GPUProc, thunk_id, f, data, chan, send_res)
        # for now, use a dedicated process to talk to GPUs
        unwrapped_data = map(unwrap_af, data)
        remotecall(do_task, p.pid, ctx, p, thunk_id, f, unwrapped_data, chan, send_res)
    end
    function Base.serialize(io::SerializationState, x::AFWrap)
        println("Hello!")
        Serializer.serialize_type(io, AFWrap)
        Base.serialize(io, Array(x.val))
    end
    function Base.deserialize(io::SerializationState, ::Type{AFWrap})
        deserialize(io) |> AFWrap
    end

end

