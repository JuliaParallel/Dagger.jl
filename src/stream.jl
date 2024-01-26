mutable struct StreamStore{T}
    waiters::Vector{Int}
    buffers::Dict{Int,Vector{Any}}
    open::Bool
    lock::Threads.Condition
    StreamStore{T}() where T =
        new{T}(zeros(Int, 0), Dict{Int,Vector{T}}(),
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
function Base.put!(store::StreamStore{T}, @nospecialize(value::T)) where T
    @lock store.lock begin
        while length(store.waiters) == 0 && isopen(store)
            @dagdebug nothing :stream_put "[$(uid())] no waiters, not putting"
            wait(store.lock)
        end
        if !isopen(store)
            @dagdebug nothing :stream_put "[$(uid())] closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        @dagdebug nothing :stream_put "[$(uid())] adding $value"
        for buffer in values(store.buffers)
            push!(buffer, value)
        end
        notify(store.lock)
    end
end
function Base.take!(store::StreamStore, id::UInt)
    @lock store.lock begin
        buffer = store.buffers[id]
        while length(buffer) == 0 && isopen(store, id)
            @dagdebug nothing :stream_take "[$(uid())] no elements, not taking"
            wait(store.lock)
        end
        @dagdebug nothing :stream_take "[$(uid())] wait finished"
        if !isopen(store, id)
            @dagdebug nothing :stream_take "[$(uid())] closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        value = popfirst!(buffer)
        @dagdebug nothing :stream_take "[$(uid())] value accepted"
        return value
    end
end
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
    store.open = false
    @lock store.lock notify(store.lock)
end
function add_waiters!(store::StreamStore, waiters::Vector{Int})
    @lock store.lock begin
        for w in waiters
            store.buffers[w] = Any[]
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

mutable struct Stream{T} <: AbstractChannel{T}
    ref::Chunk
    function Stream{T}() where T
        store = tochunk(StreamStore{T}())
        return new{T}(store)
    end
end
Stream() = Stream{Any}()

function Base.put!(stream::Stream, @nospecialize(value))
    tls = Dagger.get_tls()
    remotecall_wait(stream.ref.handle.owner, stream.ref.handle, value) do ref, value
        Dagger.set_tls!(tls)
        @nospecialize value
        store = MemPool.poolget(ref)::StreamStore
        put!(store, value)
    end
end
function Base.take!(stream::Stream{T}, id::UInt) where T
    tls = Dagger.get_tls()
    return remotecall_fetch(stream.ref.handle.owner, stream.ref.handle) do ref
        Dagger.set_tls!(tls)
        store = MemPool.poolget(ref)::StreamStore
        return take!(store, id)::T
    end
end
function Base.isopen(stream::Stream, id::UInt)::Bool
    return remotecall_fetch(stream.ref.handle.owner, stream.ref.handle) do ref
        return isopen(MemPool.poolget(ref)::StreamStore, id)
    end
end
function Base.close(stream::Stream)
    remotecall_wait(stream.ref.handle.owner, stream.ref.handle) do ref
        close(MemPool.poolget(ref)::StreamStore)
    end
end
function add_waiters!(stream::Stream, waiters::Vector{Int})
    remotecall_wait(stream.ref.handle.owner, stream.ref.handle) do ref
        add_waiters!(MemPool.poolget(ref)::StreamStore, waiters)
    end
end
add_waiters!(stream::Stream, waiter::Integer) =
    add_waiters!(stream::Stream, Int[waiter])
function remove_waiters!(stream::Stream, waiters::Vector{Int})
    remotecall_wait(stream.ref.handle.owner, stream.ref.handle) do ref
        remove_waiters!(MemPool.poolget(ref)::StreamStore, waiters)
    end
end
remove_waiters!(stream::Stream, waiter::Integer) =
    remove_waiters!(stream::Stream, Int[waiter])

function migrate_stream!(stream::Stream, w::Integer=myid())
    # Take lock to prevent any further modifications
    # N.B. Serialization automatically unlocks
    remotecall_wait(stream.ref.handle.owner, stream.ref.handle) do ref
        lock((MemPool.poolget(ref)::StreamStore).lock)
    end

    # Perform migration of the StreamStore
    # MemPool will block access to the new ref until the migration completes
    if stream.ref.handle.owner != w
        MemPool.migrate!(stream.ref.handle, w)
    end
end

struct NullStream end
Base.put!(ns::NullStream, x) = nothing
Base.take!(ns::NullStream) = throw(ConcurrencyViolationError("Cannot `take!` from a `NullStream`"))

mutable struct StreamWrapper{S}
    stream::S
    open::Bool
    StreamWrapper(stream::S) where S = new{S}(stream, true)
end
Base.isopen(sw::StreamWrapper) = sw.open
Base.close(sw::StreamWrapper) = (sw.open = false;)
function Base.put!(sw::StreamWrapper, x)
    isopen(sw) || throw(InvalidStateException("Stream is closed.", :closed))
    put!(sw.stream, x)
end
function Base.take!(sw::StreamWrapper)
    isopen(sw) || throw(InvalidStateException("Stream is closed.", :closed))
    take!(sw.stream)
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
        T_old = map(t->(t !== Union{} && t <: FinishedStreaming) ? only(t.parameters) : t, T_old)
        # We treat non-dominating error paths as unreachable
        T_old = filter(t->t !== Union{}, T_old)
        T = task.metadata.return_type = !isempty(T_old) ? Union{T_old...} : Any
        if haskey(spec.options, :stream)
            if spec.options.stream !== nothing
                # Use the user-provided stream
                @warn "Replace StreamWrapper with Stream" maxlog=1
                stream = StreamWrapper(spec.options.stream)
            else
                # Use a non-readable, non-writing stream
                stream = StreamWrapper(NullStream())
            end
            spec.options = NamedTuple(filter(opt -> opt[1] != :stream, Base.pairs(spec.options)))
        else
            # Create a built-in Stream object
            stream = Stream{T}()
        end
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

struct FinishedStreaming{T}
    value::Union{Some{T},Nothing}
end
finish_streaming(value) = FinishedStreaming{Any}(Some{T}(value))
finish_streaming() = FinishedStreaming{Union{}}(nothing)

struct StreamingFunction{F, S}
    f::F
    stream::S
end
function (sf::StreamingFunction)(args...; kwargs...)
    @nospecialize sf args kwargs
    result = nothing
    thunk_id = tid()
    @warn "Fetch from worker 1 more efficiently" maxlog=1
    uid = remotecall_fetch(1, thunk_id) do thunk_id
        lock(Sch.EAGER_ID_MAP) do id_map
            for (uid, otid) in id_map
                if thunk_id == otid
                    return uid
                end
            end
        end
    end
    if sf.stream isa Stream
        migrate_stream!(sf.stream)
    end
    try
        kwarg_names = map(name->Val{name}(), map(first, (kwargs...,)))
        kwarg_values = map(last, (kwargs...,))
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
        end

        # Ensure downstream tasks also terminate
        @dagdebug nothing :stream_close "[$uid] closed stream"
        close(sf.stream)
    end
end
# N.B We specialize to minimize/eliminate allocations
function stream!(sf::StreamingFunction, uid,
                 args::Tuple, kwarg_names::Tuple, kwarg_values::Tuple)
    while true
    #@time begin
        # Get values from Stream args/kwargs
        stream_args = _stream_take_values!(args)
        stream_kwarg_values = _stream_take_values!(kwarg_values)
        stream_kwargs = _stream_namedtuple(kwarg_names, stream_kwarg_values)

        # Run a single cycle of f
        stream_result = sf.f(stream_args...; stream_kwargs...)

        # Exit streaming on graceful request
        if stream_result isa FinishedStreaming
            @info "Terminating!"
            if stream_result.value !== nothing
                value = something(stream_result.value)
                put!(sf.stream, value)
                return value
            end
            return nothing
        end

        # Put the result into the output stream
        put!(sf.stream, stream_result)
    #end
    end
end
function _stream_take_values!(args)
    return ntuple(length(args)) do idx
        arg = args[idx]
        if arg isa Stream
            take!(arg, uid)
        elseif arg isa Union{AbstractChannel,RemoteChannel,StreamWrapper} # FIXME: Use trait query
            take!(arg)
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
        if !haskey(self_streams, task.uid)
            continue
        end

        # Adapt args to accept Stream output of other streaming tasks
        for (idx, (pos, arg)) in enumerate(spec.args)
            if arg isa EagerThunk
                if haskey(self_streams, arg.uid)
                    other_stream = self_streams[arg.uid]
                    spec.args[idx] = pos => other_stream
                    changes = get!(stream_waiter_changes, arg.uid) do
                        Int[]
                    end
                    push!(changes, task.uid)
                elseif (other_stream = task_to_stream(arg.uid)) !== nothing
                    spec.args[idx] = pos => other_stream
                    changes = get!(stream_waiter_changes, arg.uid) do
                        Int[]
                    end
                    push!(changes, task.uid)
                end
            end
        end
    end

    # Adjust waiter count of Streams with dependencies
    for (uid, waiters) in stream_waiter_changes
        stream = task_to_stream(uid)
        if stream isa Stream # FIXME: Use trait query
            add_waiters!(stream, waiters)
        end
    end
end

# TODO: Allow stopping arbitrary tasks
kill!(t::EagerThunk) = close(task_to_stream(t.uid))
