mutable struct DTaskSpec
    f
    args::Vector{Pair{Union{Symbol,Nothing},Any}}
    options::NamedTuple
end

abstract type AbstractTaskQueue end

function enqueue! end

struct DefaultTaskQueue <: AbstractTaskQueue end
enqueue!(::DefaultTaskQueue, spec::Pair{DTaskSpec,DTask}) =
    eager_launch!(spec)
enqueue!(::DefaultTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}}) =
    eager_launch!(specs)

enqueue!(spec::Pair{DTaskSpec,DTask}) =
    enqueue!(get_options(:task_queue, DefaultTaskQueue()), spec)
enqueue!(specs::Vector{Pair{DTaskSpec,DTask}}) =
    enqueue!(get_options(:task_queue, DefaultTaskQueue()), specs)

struct LazyTaskQueue <: AbstractTaskQueue
    tasks::Vector{Pair{DTaskSpec,DTask}}
    LazyTaskQueue() = new(Pair{DTaskSpec,DTask}[])
end
function enqueue!(queue::LazyTaskQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.tasks, spec)
end
function enqueue!(queue::LazyTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    append!(queue.tasks, specs)
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
    InOrderTaskQueue(upper_queue) = new(upper_queue,
                                        Set{DTask}())
end
function _add_prev_deps!(queue::InOrderTaskQueue, spec::DTaskSpec)
    # Add previously-enqueued task(s) to this task's syncdeps
    opts = spec.options
    syncdeps = get(Set{Any}, opts, :syncdeps)
    for task in queue.prev_tasks
        push!(syncdeps, task)
    end
    spec.options = merge(opts, (;syncdeps,))
end
function enqueue!(queue::InOrderTaskQueue, spec::Pair{DTaskSpec,DTask})
    if length(queue.prev_tasks) > 0
        _add_prev_deps!(queue, first(spec))
        empty!(queue.prev_tasks)
    end
    push!(queue.prev_tasks, last(spec))
    enqueue!(queue.upper_queue, spec)
end
function enqueue!(queue::InOrderTaskQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    if length(queue.prev_tasks) > 0
        for (spec, task) in specs
            _add_prev_deps!(queue, spec)
        end
        empty!(queue.prev_tasks)
    end
    for (spec, task) in specs
        push!(queue.prev_tasks, task)
    end
    enqueue!(queue.upper_queue, specs)
end
function spawn_sequential(f::Base.Callable)
    queue = InOrderTaskQueue(get_options(:task_queue, DefaultTaskQueue()))
    return with_options(f; task_queue=queue)
end

struct WaitAllQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    tasks::Vector{DTask}
end
function enqueue!(queue::WaitAllQueue, spec::Pair{DTaskSpec,DTask})
    push!(queue.tasks, spec[2])
    enqueue!(queue.upper_queue, spec)
end
function enqueue!(queue::WaitAllQueue, specs::Vector{Pair{DTaskSpec,DTask}})
    for (_, task) in specs
        push!(queue.tasks, task)
    end
    enqueue!(queue.upper_queue, specs)
end
function wait_all(f; check_errors::Bool=false)
    queue = WaitAllQueue(get_options(:task_queue, DefaultTaskQueue()), DTask[])
    result = with_options(f; task_queue=queue)
    for task in queue.tasks
        if check_errors
            fetch(task; raw=true)
        else
            wait(task)
        end
    end
    return result
end
