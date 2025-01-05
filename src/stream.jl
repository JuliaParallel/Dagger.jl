mutable struct StreamStore{T,B}
    uid::UInt
    waiters::Vector{Int}
    input_streams::Dict{UInt,Any} # FIXME: Concrete type
    output_streams::Dict{UInt,Any} # FIXME: Concrete type
    input_buffers::Dict{UInt,B}
    output_buffers::Dict{UInt,B}
    input_buffer_amount::Int
    output_buffer_amount::Int
    input_fetchers::Dict{UInt,Any}
    output_fetchers::Dict{UInt,Any}
    open::Bool
    migrating::Bool
    lock::Threads.Condition
    StreamStore{T,B}(uid::UInt, input_buffer_amount::Integer, output_buffer_amount::Integer) where {T,B} =
        new{T,B}(uid, zeros(Int, 0),
                 Dict{UInt,Any}(), Dict{UInt,Any}(),
                 Dict{UInt,B}(), Dict{UInt,B}(),
                 input_buffer_amount, output_buffer_amount,
                 Dict{UInt,Any}(), Dict{UInt,Any}(),
                 true, false, Threads.Condition())
end

function tid_to_uid(thunk_id)
    lock(Sch.EAGER_ID_MAP) do id_map
        for (uid, otid) in id_map
            if thunk_id == otid
                return uid
            end
        end
    end
end

function Base.put!(store::StreamStore{T,B}, value) where {T,B}
    thunk_id = STREAM_THUNK_ID[]
    @lock store.lock begin
        if !isopen(store)
            @dagdebug thunk_id :stream "closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        @dagdebug thunk_id :stream "adding $value ($(length(store.output_streams)) outputs)"
        for output_uid in keys(store.output_streams)
            if !haskey(store.output_buffers, output_uid)
                initialize_output_stream!(store, output_uid)
            end
            buffer = store.output_buffers[output_uid]
            while isfull(buffer)
                if !isopen(store)
                    @dagdebug thunk_id :stream "closed!"
                    throw(InvalidStateException("Stream is closed", :closed))
                end
                @dagdebug thunk_id :stream "buffer full ($(length(buffer)) values), waiting"
                wait(store.lock)
                if !isfull(buffer)
                    @dagdebug thunk_id :stream "buffer has space ($(length(buffer)) values), continuing"
                end
                task_may_cancel!()
            end
            put!(buffer, value)
        end
        notify(store.lock)
    end
end

function Base.take!(store::StreamStore, id::UInt)
    thunk_id = STREAM_THUNK_ID[]
    @lock store.lock begin
        if !haskey(store.output_buffers, id)
            @assert haskey(store.output_streams, id)
            error("Must first check isempty(store, id) before taking from a stream")
        end
        buffer = store.output_buffers[id]
        while isempty(buffer) && isopen(store, id)
            @dagdebug thunk_id :stream "no elements, not taking"
            wait(store.lock)
            task_may_cancel!()
        end
        @dagdebug thunk_id :stream "wait finished"
        if !isopen(store, id)
            @dagdebug thunk_id :stream "closed!"
            throw(InvalidStateException("Stream is closed", :closed))
        end
        unlock(store.lock)
        value = try
            take!(buffer)
        finally
            lock(store.lock)
        end
        @dagdebug thunk_id :stream "value accepted"
        notify(store.lock)
        return value
    end
end

"""
Returns whether the store is actively open. Only check this when deciding if
new values can be pushed.
"""
Base.isopen(store::StreamStore) = store.open

"""
Returns whether the store is actively open, or if closing, still has remaining
messages for `id`. Only check this when deciding if existing values can be
taken.
"""
function Base.isopen(store::StreamStore, id::UInt)
    @lock store.lock begin
        if !haskey(store.output_buffers, id)
            @assert haskey(store.output_streams, id)
            return store.open
        end
        if !isempty(store.output_buffers[id])
            return true
        end
        return store.open
    end
end

function Base.close(store::StreamStore)
    @lock store.lock begin
        store.open || return
        store.open = false
        for buffer in values(store.input_buffers)
            close(buffer)
        end
        for buffer in values(store.output_buffers)
            close(buffer)
        end
        notify(store.lock)
    end
end

# FIXME: Just pass Stream directly, rather than its uid
function add_waiters!(store::StreamStore{T,B}, waiters::Vector{Pair{UInt,Any}}) where {T,B}
    our_uid = store.uid
    @lock store.lock begin
        for (output_uid, output_fetcher) in waiters
            store.output_streams[output_uid] = task_to_stream(output_uid)
            push!(store.waiters, output_uid)
            store.output_fetchers[output_uid] = output_fetcher
        end
        notify(store.lock)
    end
end

function remove_waiters!(store::StreamStore, waiters::Vector{UInt})
    @lock store.lock begin
        for w in waiters
            delete!(store.output_buffers, w)
            idx = findfirst(wo->wo==w, store.waiters)
            deleteat!(store.waiters, idx)
            delete!(store.input_streams, w)
        end
        notify(store.lock)
    end
end

mutable struct Stream{T,B}
    uid::UInt
    store::Union{StreamStore{T,B},Nothing}
    store_ref::Chunk
    function Stream{T,B}(uid::UInt, input_buffer_amount::Integer, output_buffer_amount::Integer) where {T,B}
        # Creates a new output stream
        store = StreamStore{T,B}(uid, input_buffer_amount, output_buffer_amount)
        store_ref = tochunk(store)
        return new{T,B}(uid, store, store_ref)
    end
    function Stream(stream::Stream{T,B}) where {T,B}
        # References an existing output stream
        return new{T,B}(stream.uid, nothing, stream.store_ref)
    end
end

struct StreamingValue{B}
    buffer::B
end
Base.take!(sv::StreamingValue) = take!(sv.buffer)

function initialize_input_stream!(our_store::StreamStore{OT,OB}, input_stream::Stream{IT,IB}) where {IT,OT,IB,OB}
    input_uid = input_stream.uid
    our_uid = our_store.uid
    local buffer, input_fetcher
    @lock our_store.lock begin
        if haskey(our_store.input_buffers, input_uid)
            return StreamingValue(our_store.input_buffers[input_uid])
        end

        buffer = initialize_stream_buffer(OB, IT, our_store.input_buffer_amount)
        # FIXME: Also pass a RemoteChannel to track remote closure
        our_store.input_buffers[input_uid] = buffer
        input_fetcher = our_store.input_fetchers[input_uid]
    end
    thunk_id = STREAM_THUNK_ID[]
    tls = get_tls()
    Sch.errormonitor_tracked("streaming input: $input_uid -> $our_uid", Threads.@spawn begin
        set_tls!(tls)
        STREAM_THUNK_ID[] = thunk_id
        try
            while isopen(our_store)
                stream_pull_values!(input_fetcher, IT, our_store, input_stream, buffer)
            end
        catch err
            unwrapped_err = Sch.unwrap_nested_exception(err)
            if unwrapped_err isa InterruptException || (unwrapped_err isa InvalidStateException && !isopen(buffer))
                return
            else
                rethrow()
            end
        finally
            @dagdebug STREAM_THUNK_ID[] :stream "input stream closed"
        end
    end)
    return StreamingValue(buffer)
end
initialize_input_stream!(our_store::StreamStore, arg) = arg
function initialize_output_stream!(our_store::StreamStore{T,B}, output_uid::UInt) where {T,B}
    @assert islocked(our_store.lock)
    @dagdebug STREAM_THUNK_ID[] :stream "initializing output stream $output_uid"
    buffer = initialize_stream_buffer(B, T, our_store.output_buffer_amount)
    our_store.output_buffers[output_uid] = buffer
    our_uid = our_store.uid
    output_stream = our_store.output_streams[output_uid]
    output_fetcher = our_store.output_fetchers[output_uid]
    thunk_id = STREAM_THUNK_ID[]
    tls = get_tls()
    Sch.errormonitor_tracked("streaming output: $our_uid -> $output_uid", Threads.@spawn begin
        set_tls!(tls)
        STREAM_THUNK_ID[] = thunk_id
        try
            while true
                if !isopen(our_store) && isempty(buffer)
                    # Only exit if the buffer is empty; otherwise, we need to
                    # continue draining it
                    break
                end
                stream_push_values!(output_fetcher, T, our_store, output_stream, buffer)
            end
        catch err
            unwrapped_err = Sch.unwrap_nested_exception(err)
            if unwrapped_err isa InterruptException || (unwrapped_err isa InvalidStateException && !isopen(buffer))
                return
            else
                rethrow()
            end
        finally
            @dagdebug thunk_id :stream "output stream closed"
        end
    end)
end

Base.put!(stream::Stream, @nospecialize(value)) = put!(stream.store, value)

function Base.isopen(stream::Stream, id::UInt)::Bool
    return MemPool.access_ref(stream.store_ref.handle, id) do store, id
        return isopen(store::StreamStore, id)
    end
end

function Base.close(stream::Stream)
    MemPool.access_ref(stream.store_ref.handle) do store
        close(store::StreamStore)
        return
    end
    return
end

function add_waiters!(stream::Stream, waiters::Vector{Pair{UInt,Any}})
    MemPool.access_ref(stream.store_ref.handle, waiters) do store, waiters
        add_waiters!(store::StreamStore, waiters)
        return
    end
    return
end

function remove_waiters!(stream::Stream, waiters::Vector{UInt})
    MemPool.access_ref(stream.store_ref.handle, waiters) do store, waiters
        remove_waiters!(store::StreamStore, waiters)
        return
    end
    return
end

struct StreamingFunction{F, S}
    f::F
    stream::S
    max_evals::Int

    StreamingFunction(f::F, stream::S, max_evals) where {F, S} =
        new{F, S}(f, stream, max_evals)
end

function migrate_stream!(stream::Stream, w::Integer=myid())
    # Perform migration of the StreamStore
    # MemPool will block access to the new ref until the migration completes
    # FIXME: Do this ownership check with MemPool.access_ref,
    # in case stream was already migrated
    if stream.store_ref.handle.owner != w
        thunk_id = STREAM_THUNK_ID[]
        @dagdebug thunk_id :stream "Beginning migration... ($(length(stream.store.input_streams)) -> $(length(stream.store.output_streams)))"

        # TODO: Wire up listener to ferry cancel_token notifications to remote
        # worker once migrations occur during runtime
        tls = poolset(get_tls())
        @assert w == myid() "Only pull-based migration is currently supported"
        #remote_cancel_token = clone_cancel_token_remote(get_tls().cancel_token, worker_id)

        new_store_ref = MemPool.migrate!(stream.store_ref.handle, w;
                                         pre_migration=store->begin
                                             # Lock store to prevent any further modifications
                                             # N.B. Serialization automatically unlocks the migrated copy
                                             lock((store::StreamStore).lock)

                                             # Return the serializeable unsent inputs/outputs. We can't send the
                                             # buffers themselves because they may be mmap'ed or something.
                                             unsent_inputs = Dict(uid => collect!(buffer) for (uid, buffer) in store.input_buffers)
                                             unsent_outputs = Dict(uid => collect!(buffer) for (uid, buffer) in store.output_buffers)
                                             empty!(store.input_buffers)
                                             empty!(store.output_buffers)
                                             return (unsent_inputs, unsent_outputs)
                                         end,
                                         dest_post_migration=(store, unsent)->begin
                                             # Initialize the StreamStore on the destination with the unsent inputs/outputs.
                                             STREAM_THUNK_ID[] = thunk_id
                                             @assert !in_task()

                                             # N.B. It's not very valid to share TLS across tasks, but we do this
                                             # so that input/output tasks have access to TID and cancel token
                                             set_tls!(copy(MemPool.access_ref(identity, tls; local_only=true)))

                                             unsent_inputs, unsent_outputs = unsent
                                             for (input_uid, inputs) in unsent_inputs
                                                 input_stream = store.input_streams[input_uid]
                                                 initialize_input_stream!(store, input_stream)
                                                 for item in inputs
                                                     put!(store.input_buffers[input_uid], item)
                                                 end
                                             end
                                             for (output_uid, outputs) in unsent_outputs
                                                 initialize_output_stream!(store, output_uid)
                                                 for item in outputs
                                                     put!(store.output_buffers[output_uid], item)
                                                 end
                                             end

                                             # Reset the state of this new store
                                             store.open = true
                                             store.migrating = false
                                         end,
                                         post_migration=store->begin
                                             # Indicate that this store has migrated
                                             store.migrating = true
                                             store.open = false

                                             # Unlock the store
                                             unlock((store::StreamStore).lock)
                                         end)
        if w == myid()
            stream.store_ref.handle = new_store_ref # FIXME: It's not valid to mutate the Chunk handle, but we want to update this to enable fast location queries
            stream.store = MemPool.access_ref(identity, new_store_ref; local_only=true)
        end

        @dagdebug thunk_id :stream "Migration complete ($(length(stream.store.input_streams)) -> $(length(stream.store.output_streams)))"
    end
end

struct StreamingTaskQueue <: AbstractTaskQueue
    tasks::Vector{Pair{DTaskSpec,DTask}}
    self_streams::Dict{UInt,Any}
    StreamingTaskQueue() = new(Pair{DTaskSpec,DTask}[],
                               Dict{UInt,Any}())
end

function enqueue!(queue::StreamingTaskQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.tasks, spec)
    initialize_streaming!(queue.self_streams, spec...)
end

function enqueue!(queue::StreamingTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    append!(queue.tasks, specs)
    for (spec, task) in specs
        initialize_streaming!(queue.self_streams, spec, task)
    end
end

function initialize_streaming!(self_streams, spec, task)
    @assert !isa(value(spec.fargs[1]), StreamingFunction) "Task is already in streaming form"

    # Calculate the return type of the called function
    T_old = Base.uniontypes(task.metadata.return_type)
    T_old = map(t->(t !== Union{} && t <: FinishStream) ? first(t.parameters) : t, T_old)
    # N.B. We treat non-dominating error paths as unreachable
    T_old = filter(t->t !== Union{}, T_old)
    T = task.metadata.return_type = !isempty(T_old) ? Union{T_old...} : Any

    # Get input buffer configuration
    input_buffer_amount = something(spec.options.stream_input_buffer_amount, 1)
    if input_buffer_amount <= 0
        throw(ArgumentError("Input buffering is required; please specify a `stream_input_buffer_amount` greater than 0"))
    end

    # Get output buffer configuration
    output_buffer_amount = something(spec.options.stream_output_buffer_amount, 1)
    if output_buffer_amount <= 0
        throw(ArgumentError("Output buffering is required; please specify a `stream_output_buffer_amount` greater than 0"))
    end

    # Create the Stream
    buffer_type = something(spec.options.stream_buffer_type, ProcessRingBuffer)
    stream = Stream{T,buffer_type}(task.uid, input_buffer_amount, output_buffer_amount)
    self_streams[task.uid] = stream

    # Get max evaluation count
    max_evals = something(spec.options.stream_max_evals, -1)
    if max_evals == 0
        throw(ArgumentError("stream_max_evals cannot be 0"))
    end

    # Wrap the function in a StreamingFunction
    spec.fargs[1].value = StreamingFunction(value(spec.fargs[1]), stream, max_evals)

    # Mark the task as non-blocking
    spec.options.occupancy = @something(spec.options.occupancy, Dict())
    spec.options.occupancy[Any] = 0

    # Register Stream globally
    remotecall_wait(1, task.uid, stream) do uid, stream
        lock(EAGER_THUNK_STREAMS) do global_streams
            global_streams[uid] = stream
        end
    end
end

"""
Starts a streaming region, within which all tasks run continuously and
concurrently. Any `DTask` argument that is itself a streaming task will be
treated as a streaming input/output. The streaming region will automatically
handle the buffering and synchronization of these tasks' values.

# Keyword Arguments
- `teardown::Bool=true`: If `true`, the streaming region will automatically
  cancel all tasks if any task fails or is cancelled. Otherwise, a failing task
  will not cancel the other tasks, which will continue running.
"""
function spawn_streaming(f::Base.Callable; teardown::Bool=true)
    queue = StreamingTaskQueue()
    result = with_options(f; task_queue=queue)
    if length(queue.tasks) > 0
        finalize_streaming!(queue.tasks, queue.self_streams)
        enqueue!(queue.tasks)

        if teardown
            # Start teardown monitor
            dtasks = map(last, queue.tasks)::Vector{DTask}
            Sch.errormonitor_tracked("streaming teardown", Threads.@spawn begin
                # Wait for any task to finish
                waitany(dtasks)

                # Cancel all tasks
                for task in dtasks
                    cancel!(task; graceful=false)
                end
            end)
        end
    end
    return result
end

struct FinishStream{T,R}
    value::Union{Some{T},Nothing}
    result::R
end

finish_stream(value::T; result::R=nothing) where {T,R} = FinishStream{T,R}(Some{T}(value), result)

finish_stream(; result::R=nothing) where R = FinishStream{Union{},R}(nothing, result)

const STREAM_THUNK_ID = TaskLocalValue{Int}(()->0)

chunktype(sf::StreamingFunction{F}) where F = F

struct StreamMigrating end

function (sf::StreamingFunction)(args...; kwargs...)
    thunk_id = Sch.sch_handle().thunk_id.id
    STREAM_THUNK_ID[] = thunk_id

    # Migrate our output stream store to this worker
    if sf.stream isa Stream
        remote_cancel_token = migrate_stream!(sf.stream)
    end

    @label start
    @dagdebug thunk_id :stream "Starting StreamingFunction"
    worker_id = sf.stream.store_ref.handle.owner # FIXME: Not valid to access the owner directly
    result = if worker_id == myid()
        _run_streamingfunction(nothing, nothing, sf, args...; kwargs...)
    else
        tls = get_tls()
        remotecall_fetch(_run_streamingfunction, worker_id, tls, remote_cancel_token, sf, args...; kwargs...)
    end
    if result === StreamMigrating()
        @goto start
    end
    return result
end

function _run_streamingfunction(tls, cancel_token, sf, args...; kwargs...)
    @nospecialize sf args kwargs

    store = sf.stream.store = MemPool.access_ref(identity, sf.stream.store_ref.handle; local_only=true)
    @assert isopen(store)

    if tls !== nothing
        # Setup TLS on this new task
        tls.cancel_token = MemPool.access_ref(identity, cancel_token; local_only=true)
        set_tls!(tls)
    end

    thunk_id = Sch.sch_handle().thunk_id.id
    STREAM_THUNK_ID[] = thunk_id

    # FIXME: Remove when scheduler is distributed
    uid = remotecall_fetch(1, thunk_id) do thunk_id
        lock(Sch.EAGER_ID_MAP) do id_map
            for (uid, otid) in id_map
                if thunk_id == otid
                    return uid
                end
            end
        end
    end

    try
        # TODO: This kwarg song-and-dance is required to ensure that we don't
        # allocate boxes within `stream!`, when possible
        kwarg_names = map(name->Val{name}(), map(first, (kwargs...,)))
        kwarg_values = map(last, (kwargs...,))
        args = map(arg->initialize_input_stream!(store, arg), args)
        kwarg_values = map(kwarg->initialize_input_stream!(store, kwarg), kwarg_values)
        return stream!(sf, uid, (args...,), kwarg_names, kwarg_values)
    finally
        if !sf.stream.store.migrating
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
                @dagdebug thunk_id :stream "dropping waiter"
                remove_waiters!(stream, uid)
                @dagdebug thunk_id :stream "dropped waiter"
            end

            # Ensure downstream tasks also terminate
            close(sf.stream)
            @dagdebug thunk_id :stream "closed stream store"
        end
    end
end

# N.B We specialize to minimize/eliminate allocations
function stream!(sf::StreamingFunction, uid,
                 args::Tuple, kwarg_names::Tuple, kwarg_values::Tuple)
    f = move(task_processor(), sf.f)
    counter = 0

    while true
        # Yield to other (streaming) tasks
        yield()

        # Exit streaming on cancellation
        task_may_cancel!()

        # Exit streaming on migration
        if sf.stream.store.migrating
            error("FIXME: max_evals should be retained")
            @dagdebug STREAM_THUNK_ID[] :stream "returning for migration"
            return StreamMigrating()
        end

        # Get values from Stream args/kwargs
        stream_args = _stream_take_values!(args)
        stream_kwarg_values = _stream_take_values!(kwarg_values)
        stream_kwargs = _stream_namedtuple(kwarg_names, stream_kwarg_values)

        if length(stream_args) > 0 || length(stream_kwarg_values) > 0
            # Notify tasks that input buffers may have space
            @lock sf.stream.store.lock notify(sf.stream.store.lock)
        end

        # Run a single cycle of f
        counter += 1
        @dagdebug STREAM_THUNK_ID[] :stream "executing $f (eval $counter)"
        stream_result = f(stream_args...; stream_kwargs...)

        # Exit streaming on graceful request
        if stream_result isa FinishStream
            if stream_result.value !== nothing
                value = something(stream_result.value)
                put!(sf.stream, value)
            end
            @dagdebug STREAM_THUNK_ID[] :stream "voluntarily returning"
            return stream_result.result
        end

        # Put the result into the output stream
        put!(sf.stream, stream_result)

        # Exit streaming on eval limit
        if sf.max_evals > 0 && counter >= sf.max_evals
            @dagdebug STREAM_THUNK_ID[] :stream "max evals reached (eval $counter)"
            return
        end
    end
end

function _stream_take_values!(args)
    return ntuple(length(args)) do idx
        arg = args[idx]
        if arg isa StreamingValue
            return take!(arg)
        else
            return arg
        end
    end
end

@inline @generated function _stream_namedtuple(kwarg_names::Tuple,
                                               stream_kwarg_values::Tuple)
    name_ex = Expr(:tuple, map(name->QuoteNode(name.parameters[1]), kwarg_names.parameters)...)
    NT = :(NamedTuple{$name_ex,$stream_kwarg_values})
    return :($NT(stream_kwarg_values))
end

# Default for buffers, can be customized
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

function finalize_streaming!(tasks::Vector{Pair{DTaskSpec,DTask}}, self_streams)
    stream_waiter_changes = Dict{UInt,Vector{Pair{UInt,Any}}}()

    for (spec, task) in tasks
        @assert haskey(self_streams, task.uid)
        our_stream = self_streams[task.uid]

        # Adapt args to accept Stream output of other streaming tasks
        for (idx, pos_arg) in enumerate(spec.fargs)
            arg = value(pos_arg)
            if arg isa DTask
                # Check if this is a streaming task
                if haskey(self_streams, arg.uid)
                    other_stream = self_streams[arg.uid]
                else
                    other_stream = task_to_stream(arg.uid)
                end

                if other_stream !== nothing
                    # Generate Stream handle for input
                    # FIXME: Be configurable
                    input_fetcher = RemoteChannelFetcher()
                    other_stream_handle = Stream(other_stream)
                    pos_arg.value = other_stream_handle
                    our_stream.store.input_streams[arg.uid] = other_stream_handle
                    our_stream.store.input_fetchers[arg.uid] = input_fetcher

                    # Add this task as a waiter for the associated output Stream
                    changes = get!(stream_waiter_changes, arg.uid) do
                        Pair{UInt,Any}[]
                    end
                    push!(changes, task.uid => input_fetcher)
                end
            end
        end
    end

    # Notify Streams of any new waiters
    for (uid, waiters) in stream_waiter_changes
        stream = task_to_stream(uid)
        add_waiters!(stream, waiters)
    end
end
