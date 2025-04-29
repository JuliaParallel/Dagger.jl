function _mutable_inner(@nospecialize(f), proc, scope)
    result = f()
    return Ref(Dagger.tochunk(result, proc, scope))
end

"""
    mutable(f::Base.Callable; worker, processor, scope) -> Chunk

Calls `f()` on the specified worker or processor, returning a `Chunk`
referencing the result with the specified scope `scope`.
"""
function mutable(@nospecialize(f); worker=nothing, processor=nothing, scope=nothing)
    if processor === nothing
        if worker === nothing
            processor = OSProc()
        else
            processor = OSProc(worker)
        end
    else
        @assert worker === nothing "mutable: Can't mix worker and processor"
    end
    if scope === nothing
        scope = processor isa OSProc ? ProcessScope(processor) : ExactScope(processor)
    end
    return fetch(Dagger.@spawn scope=scope _mutable_inner(f, processor, scope))[]
end

"""
    @mutable [worker=1] [processor=OSProc()] [scope=ProcessorScope()] f()

Helper macro for [`mutable()`](@ref).
"""
macro mutable(exs...)
    opts = esc.(exs[1:end-1])
    ex = exs[end]
    quote
        let f = @noinline ()->$(esc(ex))
            $mutable(f; $(opts...))
        end
    end
end