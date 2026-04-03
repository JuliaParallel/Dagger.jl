"""
    TaskID

Globally-unique task identifier containing the originating worker ID and a
worker-local sequential counter.  The local `id` counts upward from 1 on each
worker, which makes it easy to see creation order in debug output.
"""
struct TaskID
    worker::Int
    id::Int
end

const TASKID_ZERO = TaskID(0, 0)

is_task_local(t::TaskID) = t.worker == myid()