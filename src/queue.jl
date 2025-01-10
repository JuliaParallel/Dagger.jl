mutable struct DTaskSpec{typed,FA<:Tuple}
    _fargs::Vector{Argument}
    _typed_fargs::FA
    options::Options
end
DTaskSpec(fargs::Vector{Argument}, options::Options) =
    DTaskSpec{false, Tuple{}}(fargs, (), options)
DTaskSpec(fargs::FA, options::Options) where FA  =
    DTaskSpec{true, FA}(Argument[], fargs, options)
is_typed(spec::DTaskSpec{typed}) where typed = typed
function Base.getproperty(spec::DTaskSpec{typed}, field::Symbol) where typed
    if field === :fargs
        if typed
            return getfield(spec, :_typed_fargs)
        else
            return getfield(spec, :_fargs)
        end
    else
        return getfield(spec, field)
    end
end

struct DTaskPair
    spec::DTaskSpec
    task::DTask
end
is_typed(pair::DTaskPair) = is_typed(pair.spec)
Base.iterate(pair::DTaskPair) = (pair.spec, true)
function Base.iterate(pair::DTaskPair, state::Bool)
    if state
        return (pair.task, false)
    else
        return nothing
    end
end

abstract type AbstractTaskQueue end

function enqueue! end

struct DefaultTaskQueue <: AbstractTaskQueue end
enqueue!(::DefaultTaskQueue, pair::DTaskPair) =
    eager_launch!(pair)
enqueue!(::DefaultTaskQueue, pairs::Vector{DTaskPair}) =
    eager_launch!(pairs)

enqueue!(pair::DTaskPair) =
    enqueue!(get_options(:task_queue, DefaultTaskQueue()), pair)
enqueue!(pairs::Vector{DTaskPair}) =
    enqueue!(get_options(:task_queue, DefaultTaskQueue()), pairs)

struct LazyTaskQueue <: AbstractTaskQueue
    tasks::Vector{DTaskPair}
    LazyTaskQueue() = new(DTaskPair[])
end
function enqueue!(queue::LazyTaskQueue, pair::DTaskPair)
    push!(queue.tasks, pair)
end
function enqueue!(queue::LazyTaskQueue, pairs::Vector{DTaskPair})
    append!(queue.tasks, pairs)
end
function spawn_bulk(f::Base.Callable)
    queue = LazyTaskQueue()
    result = with_options(f; task_queue=queue)
    if length(queue.tasks) > 0
        enqueue!(queue.tasks)
    end
    return result
end

struct InOrderTaskQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    prev_tasks::Set{DTask}
    InOrderTaskQueue(upper_queue) = new(upper_queue, Set{DTask}())
end
function _add_prev_deps!(queue::InOrderTaskQueue, spec::DTaskSpec)
    # Add previously-enqueued task(s) to this task's syncdeps
    opts = spec.options
    syncdeps = opts.syncdeps = @something(opts.syncdeps, Set{ThunkSyncdep}())
    for task in queue.prev_tasks
        push!(syncdeps, ThunkSyncdep(task))
    end
end
function enqueue!(queue::InOrderTaskQueue, pair::DTaskPair)
    if length(queue.prev_tasks) > 0
        _add_prev_deps!(queue, pair.spec)
        empty!(queue.prev_tasks)
    end
    push!(queue.prev_tasks, pair.task)
    enqueue!(queue.upper_queue, pair)
end
function enqueue!(queue::InOrderTaskQueue, pairs::Vector{DTaskPair})
    if length(queue.prev_tasks) > 0
        for pair in pairs
            _add_prev_deps!(queue, pair.spec)
        end
        empty!(queue.prev_tasks)
    end
    for pair in pairs
        push!(queue.prev_tasks, pair.task)
    end
    enqueue!(queue.upper_queue, pairs)
end
function spawn_sequential(f::Base.Callable)
    queue = InOrderTaskQueue(get_options(:task_queue, DefaultTaskQueue()))
    return with_options(f; task_queue=queue)
end

struct WaitAllQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    tasks::Vector{DTask}
end
function enqueue!(queue::WaitAllQueue, pair::DTaskPair)
    push!(queue.tasks, pair.task)
    enqueue!(queue.upper_queue, pair)
end
function enqueue!(queue::WaitAllQueue, pairs::Vector{DTaskPair})
    for pair in pairs
        push!(queue.tasks, pair.task)
    end
    enqueue!(queue.upper_queue, pairs)
end
function wait_all(f; check_errors::Bool=false)
    queue = WaitAllQueue(get_options(:task_queue, DefaultTaskQueue()), DTask[])
    result = with_options(f; task_queue=queue)
    for task in queue.tasks
        if check_errors
            fetch(task; move_value=false, unwrap=false, throw_on_error=true)
        else
            wait(task)
        end
    end
    return result
end
