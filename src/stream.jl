mutable struct StreamStore{T,B}
    waiters::Vector{Int}
    buffers::Dict{Int,B}
    buffer_amount::Int
    open::Bool
    lock::Threads.Condition
    StreamStore{T,B}(buffer_amount::Integer) where {T,B} =
        new{T,B}(zeros(Int, 0), Dict{Int,B}(), buffer_amount,
                 true, Threads.Condition())
end
tid() = Dagger.Sch.sch_handle().thunk_id.id
function uid()
    thunk_id = tid()
    lock(Sch.EAGER_ID_MAP) do id_map
        for (uid, otid) in id_map
            if thunk_id == otid
                return uid
            end
        end
    end
end
function Base.put!(store::StreamStore{T,B}, value) where {T,B}
    @lock store.lock begin
        if !isopen(store)
            @dagdebug nothing :stream_put "[$(uid())] closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        @dagdebug nothing :stream_put "[$(uid())] adding $value"
        for buffer in values(store.buffers)
            while isfull(buffer)
                @dagdebug nothing :stream_put "[$(uid())] buffer full, waiting"
                wait(store.lock)
            end
            put!(buffer, value)
        end
        notify(store.lock)
    end
end
function Base.take!(store::StreamStore, id::UInt)
    @lock store.lock begin
        buffer = store.buffers[id]
        while isempty(buffer) && isopen(store, id)
            @dagdebug nothing :stream_take "[$(uid())] no elements, not taking"
            wait(store.lock)
        end
        @dagdebug nothing :stream_take "[$(uid())] wait finished"
        if !isopen(store, id)
            @dagdebug nothing :stream_take "[$(uid())] closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        unlock(store.lock)
        value = try
            take!(buffer)
        finally
            lock(store.lock)
        end
        @dagdebug nothing :stream_take "[$(uid())] value accepted"
        notify(store.lock)
        return value
    end
end
Base.isempty(store::StreamStore, id::UInt) = isempty(store.buffers[id])
isfull(store::StreamStore, id::UInt) = isfull(store.buffers[id])
"Returns whether the store is actively open. Only check this when deciding if new values can be pushed."
Base.isopen(store::StreamStore) = store.open
"Returns whether the store is actively open, or if closing, still has remaining messages for `id`. Only check this when deciding if existing values can be taken."
function Base.isopen(store::StreamStore, id::UInt)
    @lock store.lock begin
        if !isempty(store.buffers[id])
            return true
        end
        return store.open
    end
end
function Base.close(store::StreamStore)
    if store.open
        store.open = false
        @lock store.lock notify(store.lock)
    end
end
function add_waiters!(store::StreamStore{T,B}, waiters::Vector{Int}) where {T,B}
    @lock store.lock begin
        for w in waiters
            buffer = initialize_stream_buffer(B, T, store.buffer_amount)
            store.buffers[w] = buffer
        end
        append!(store.waiters, waiters)
        notify(store.lock)
    end
end
function remove_waiters!(store::StreamStore, waiters::Vector{Int})
    @lock store.lock begin
        for w in waiters
            delete!(store.buffers, w)
            idx = findfirst(wo->wo==w, store.waiters)
            deleteat!(store.waiters, idx)
        end
        notify(store.lock)
    end
end

mutable struct Stream{T,B}
    store::Union{StreamStore{T,B},Nothing}
    store_ref::Chunk
    input_buffer::Union{B,Nothing}
    buffer_amount::Int
    function Stream{T,B}(buffer_amount::Integer=0) where {T,B}
        # Creates a new output stream
        store = StreamStore{T,B}(buffer_amount)
        store_ref = tochunk(store)
        return new{T,B}(store, store_ref, nothing, buffer_amount)
    end
    function Stream{B}(stream::Stream{T}, buffer_amount::Integer=0) where {T,B}
        # References an existing output stream
        return new{T,B}(nothing, stream.store_ref, nothing, buffer_amount)
    end
end
function initialize_input_stream!(stream::Stream{T,B}) where {T,B}
    stream.input_buffer = initialize_stream_buffer(B, T, stream.buffer_amount)
end

Base.put!(stream::Stream, @nospecialize(value)) =
    put!(stream.store, value)
function Base.take!(stream::Stream{T,B}, id::UInt) where {T,B}
    # FIXME: Make remote fetcher configurable
    stream_fetch_values!(RemoteFetcher, T, stream.store_ref, stream.input_buffer, id)
    return take!(stream.input_buffer)
end
function Base.isopen(stream::Stream, id::UInt)::Bool
    return remotecall_fetch(stream.store_ref.handle.owner, stream.store_ref.handle) do ref
        return isopen(MemPool.poolget(ref)::StreamStore, id)
    end
end
function Base.close(stream::Stream)
    remotecall_wait(stream.store_ref.handle.owner, stream.store_ref.handle) do ref
        close(MemPool.poolget(ref)::StreamStore)
    end
end
function add_waiters!(stream::Stream, waiters::Vector{Int})
    remotecall_wait(stream.store_ref.handle.owner, stream.store_ref.handle) do ref
        add_waiters!(MemPool.poolget(ref)::StreamStore, waiters)
    end
end
add_waiters!(stream::Stream, waiter::Integer) =
    add_waiters!(stream::Stream, Int[waiter])
function remove_waiters!(stream::Stream, waiters::Vector{Int})
    remotecall_wait(stream.store_ref.handle.owner, stream.store_ref.handle) do ref
        remove_waiters!(MemPool.poolget(ref)::StreamStore, waiters)
    end
end
remove_waiters!(stream::Stream, waiter::Integer) =
    remove_waiters!(stream::Stream, Int[waiter])

function migrate_stream!(stream::Stream, w::Integer=myid())
    if !isdefined(MemPool, :migrate!)
        @warn "MemPool migration support not enabled!\nPerformance may be degraded" maxlog=1
        return
    end

    # Perform migration of the StreamStore
    # MemPool will block access to the new ref until the migration completes
    if stream.store_ref.handle.owner != w
        # Take lock to prevent any further modifications
        # N.B. Serialization automatically unlocks
        remotecall_wait(stream.store_ref.handle.owner, stream.store_ref.handle) do ref
            lock((MemPool.poolget(ref)::StreamStore).lock)
        end

        MemPool.migrate!(stream.store_ref.handle, w)
    end
end

struct StreamingTaskQueue <: AbstractTaskQueue
    tasks::Vector{Pair{EagerTaskSpec,EagerThunk}}
    self_streams::Dict{UInt,Any}
    StreamingTaskQueue() = new(Pair{EagerTaskSpec,EagerThunk}[],
                               Dict{UInt,Any}())
end

function enqueue!(queue::StreamingTaskQueue, spec::Pair{EagerTaskSpec,EagerThunk})
    push!(queue.tasks, spec)
    initialize_streaming!(queue.self_streams, spec...)
end
function enqueue!(queue::StreamingTaskQueue, specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    append!(queue.tasks, specs)
    for (spec, task) in specs
        initialize_streaming!(queue.self_streams, spec, task)
    end
end
function initialize_streaming!(self_streams, spec, task)
    if !isa(spec.f, StreamingFunction)
        # Adapt called function for streaming and generate output Streams
        T_old = Base.uniontypes(task.metadata.return_type)
        T_old = map(t->(t !== Union{} && t <: FinishStream) ? first(t.parameters) : t, T_old)
        # We treat non-dominating error paths as unreachable
        T_old = filter(t->t !== Union{}, T_old)
        T = task.metadata.return_type = !isempty(T_old) ? Union{T_old...} : Any
        output_buffer_amount = get(spec.options, :stream_output_buffer_amount, 1)
        if output_buffer_amount <= 0
            throw(ArgumentError("Output buffering is required; please specify a `stream_output_buffer_amount` greater than 0"))
        end
        output_buffer = get(spec.options, :stream_output_buffer, ProcessRingBuffer)
        stream = Stream{T,output_buffer}(output_buffer_amount)
        spec.options = NamedTuple(filter(opt -> opt[1] != :stream_output_buffer &&
                                                opt[1] != :stream_output_buffer_amount,
                                         Base.pairs(spec.options)))
        self_streams[task.uid] = stream

        spec.f = StreamingFunction(spec.f, stream)
        spec.options = merge(spec.options, (;occupancy=Dict(Any=>0)))

        # Register Stream globally
        remotecall_wait(1, task.uid, stream) do uid, stream
            lock(EAGER_THUNK_STREAMS) do global_streams
                global_streams[uid] = stream
            end
        end
    end
end

function spawn_streaming(f::Base.Callable)
    queue = StreamingTaskQueue()
    result = with_options(f; task_queue=queue)
    if length(queue.tasks) > 0
        finalize_streaming!(queue.tasks, queue.self_streams)
        enqueue!(queue.tasks)
    end
    return result
end

struct FinishStream{T,R}
    value::Union{Some{T},Nothing}
    result::R
end
finish_stream(value::T; result::R=nothing) where {T,R} =
    FinishStream{T,R}(Some{T}(value), result)
finish_stream(; result::R=nothing) where R =
    FinishStream{Union{},R}(nothing, result)

function cancel_stream!(t::EagerThunk)
    stream = task_to_stream(t.uid)
    if stream !== nothing
        close(stream)
    end
end

struct StreamingFunction{F, S}
    f::F
    stream::S
end
chunktype(sf::StreamingFunction{F}) where F = F
function (sf::StreamingFunction)(args...; kwargs...)
    @nospecialize sf args kwargs
    result = nothing
    thunk_id = tid()
    uid = remotecall_fetch(1, thunk_id) do thunk_id
        lock(Sch.EAGER_ID_MAP) do id_map
            for (uid, otid) in id_map
                if thunk_id == otid
                    return uid
                end
            end
        end
    end

    # Migrate our output stream to this worker
    if sf.stream isa Stream
        migrate_stream!(sf.stream)
    end

    try
        # TODO: This kwarg song-and-dance is required to ensure that we don't
        # allocate boxes within `stream!`, when possible
        kwarg_names = map(name->Val{name}(), map(first, (kwargs...,)))
        kwarg_values = map(last, (kwargs...,))
        for arg in args
            if arg isa Stream
                initialize_input_stream!(arg)
            end
        end
        return stream!(sf, uid, (args...,), kwarg_names, kwarg_values)
    finally
        # Remove ourself as a waiter for upstream Streams
        streams = Set{Stream}()
        for (idx, arg) in enumerate(args)
            if arg isa Stream
                push!(streams, arg)
            end
        end
        for (idx, (pos, arg)) in enumerate(kwargs)
            if arg isa Stream
                push!(streams, arg)
            end
        end
        for stream in streams
            @dagdebug nothing :stream_close "[$uid] dropping waiter"
            remove_waiters!(stream, uid)
            @dagdebug nothing :stream_close "[$uid] dropped waiter"
        end

        # Ensure downstream tasks also terminate
        @dagdebug nothing :stream_close "[$uid] closed stream"
        close(sf.stream)
    end
end
# N.B We specialize to minimize/eliminate allocations
function stream!(sf::StreamingFunction, uid,
                 args::Tuple, kwarg_names::Tuple, kwarg_values::Tuple)
    f = move(thunk_processor(), sf.f)
    while true
        # Get values from Stream args/kwargs
        stream_args = _stream_take_values!(args, uid)
        stream_kwarg_values = _stream_take_values!(kwarg_values, uid)
        stream_kwargs = _stream_namedtuple(kwarg_names, stream_kwarg_values)

        # Run a single cycle of f
        stream_result = f(stream_args...; stream_kwargs...)

        # Exit streaming on graceful request
        if stream_result isa FinishStream
            if stream_result.value !== nothing
                value = something(stream_result.value)
                put!(sf.stream, value)
            end
            return stream_result.result
        end

        # Put the result into the output stream
        put!(sf.stream, stream_result)
    end
end
function _stream_take_values!(args, uid)
    return ntuple(length(args)) do idx
        arg = args[idx]
        if arg isa Stream
            take!(arg, uid)
        else
            arg
        end
    end
end
@inline @generated function _stream_namedtuple(kwarg_names::Tuple,
                                               stream_kwarg_values::Tuple)
    name_ex = Expr(:tuple, map(name->QuoteNode(name.parameters[1]), kwarg_names.parameters)...)
    NT = :(NamedTuple{$name_ex,$stream_kwarg_values})
    return :($NT(stream_kwarg_values))
end
initialize_stream_buffer(B, T, buffer_amount) = B{T}(buffer_amount)

const EAGER_THUNK_STREAMS = LockedObject(Dict{UInt,Any}())
function task_to_stream(uid::UInt)
    if myid() != 1
        return remotecall_fetch(task_to_stream, 1, uid)
    end
    lock(EAGER_THUNK_STREAMS) do global_streams
        if haskey(global_streams, uid)
            return global_streams[uid]
        end
        return
    end
end

function finalize_streaming!(tasks::Vector{Pair{EagerTaskSpec,EagerThunk}}, self_streams)
    stream_waiter_changes = Dict{UInt,Vector{Int}}()

    for (spec, task) in tasks
        @assert haskey(self_streams, task.uid)

        # Adapt args to accept Stream output of other streaming tasks
        for (idx, (pos, arg)) in enumerate(spec.args)
            if arg isa EagerThunk
                # Check if this is a streaming task
                if haskey(self_streams, arg.uid)
                    other_stream = self_streams[arg.uid]
                else
                    other_stream = task_to_stream(arg.uid)
                end

                if other_stream !== nothing
                    # Get input stream configs and configure input stream
                    input_buffer_amount = get(spec.options, :stream_input_buffer_amount, 1)
                    if input_buffer_amount <= 0
                        throw(ArgumentError("Input buffering is required; please specify a `stream_input_buffer_amount` greater than 0"))
                    end
                    input_buffer = get(spec.options, :stream_input_buffer, ProcessRingBuffer)
                    # FIXME: input_fetcher = get(spec.options, :stream_input_fetcher, RemoteFetcher)
                    input_stream = Stream{input_buffer}(other_stream, input_buffer_amount)

                    # Replace the EagerThunk with the input Stream
                    spec.args[idx] = pos => other_stream

                    # Add this task as a waiter for the associated output Stream
                    changes = get!(stream_waiter_changes, arg.uid) do
                        Int[]
                    end
                    push!(changes, task.uid)
                end
            end
        end

        # Filter out all streaming options
        to_filter = (:stream_input_buffer, :stream_input_buffer_amount,
                     :stream_output_buffer, :stream_output_buffer_amount)
        spec.options = NamedTuple(filter(opt -> !(opt[1] in to_filter),
                                         Base.pairs(spec.options)))
        if haskey(spec.options, :propagates)
            propagates = filter(opt -> !(opt in to_filter),
                                spec.options.propagates)
            spec.options = merge(spec.options, (;propagates))
        end
    end

    # Adjust waiter count of Streams with dependencies
    for (uid, waiters) in stream_waiter_changes
        stream = task_to_stream(uid)
        add_waiters!(stream, waiters)
    end
end
