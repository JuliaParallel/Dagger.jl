using ProgressMeter
using TaskLocalValues

struct MonitorTaskQueue <: AbstractTaskQueue
    running_tasks::Vector{WeakRef}
    MonitorTaskQueue() = new(WeakRef[])
end
function enqueue!(queue::MonitorTaskQueue, spec::Pair{EagerTaskSpec,EagerThunk})
    push!(queue.running_tasks, WeakRef(spec[2]))
    upper = get_options(:task_queue, EagerTaskQueue())
    enqueue!(upper, spec)
end

function enqueue!(queue::MonitorTaskQueue, specs::Vector{Pair{EagerTaskSpec,EagerThunk}})
    for (_, task) in specs
        push!(queue.running_tasks, WeakRef(task))
    end
    upper = get_options(:task_queue, EagerTaskQueue())
    enqueue!(upper, specs)
end

const MONITOR_QUEUE = TaskLocalValue{MonitorTaskQueue}(MonitorTaskQueue)

"Monitors and displays the progress of any still-executing tasks."
function monitor()
    queue = MONITOR_QUEUE[]
    running_tasks = queue.running_tasks
    isempty(running_tasks) && return

    ntasks = length(running_tasks)
    meter = Progress(ntasks;
                     desc="Waiting for $ntasks tasks...",
                     dt=0.01, showspeed=true)
    while !isempty(running_tasks)
        for (i, task_weak) in reverse(collect(enumerate(running_tasks)))
            task = task_weak.value
            if task === nothing || isready(task)
                next!(meter)
                deleteat!(running_tasks, i)
            end
        end
        sleep(0.01)
    end
    finish!(meter)

    return
end
