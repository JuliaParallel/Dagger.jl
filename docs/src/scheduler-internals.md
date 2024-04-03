# Scheduler Internals

Dagger's scheduler can be found primarily in the `Dagger.Sch` module. It
performs a variety of functions to support tasks and data, and as such is a
complex system. This documentation attempts to shed light on how the scheduler
works internally (from a somewhat high level), with the hope that it will help
users and contributors understand how to improve the scheduler or fix any bugs
that may arise from it.

!!! warn
    Dagger's scheduler is evolving at a rapid pace, and is a complex mix of interacting parts. As such, this documentation may become out of date very quickly, and may not reflect the current state of the scheduler. Please feel free to file PRs to correct or improve this document, but also beware that the true functionality is defined in Dagger's source!

## Core vs. Worker Schedulers

Dagger's scheduler is really two kinds of entities: the "core" scheduler, and
"worker" schedulers:

The core scheduler runs on worker 1, thread 1, and is the entrypoint to tasks
which have been submitted. The core scheduler manages all task dependencies,
notifies calls to `wait` and `fetch` of task completion, and generally performs
initial task placement. The core scheduler has cached information about each
worker and their processors, and uses that information (together with metrics
about previous tasks and other aspects of the Dagger runtime) to generate a
near-optimal just-in-time task schedule.

The worker schedulers each run as a set of tasks across all workers and all
processors, and handles data movement and task execution. Once the core
scheduler has scheduled and launched a task, it arrives at the worker scheduler
for handling. The worker scheduler will pass the task to a queue for the
assigned processor, where it will wait until the processor has a sufficient
amount of "occupancy" for the task. Once the processor is ready for the task,
it will first fetch all of the task's arguments from other workers, and then it
will execute the task, package the task's result into a `Chunk`, and pass that
back to the core scheduler.

## Core: Basics

The core scheduler contains a single internal instance of type `ComputeState`,
which maintains (among many other things) all necessary state to represent the
set of waiting, ready, and running tasks, cached task results, and maps of
interdependencies between tasks. It uses Julia's task infrastructure to
asynchronously send work requests to remote Julia processes, and uses a
`RemoteChannel` as an inbound queue for completed work.

There is an outer loop which drives the scheduler, which continues executing
either eternally (excepting any internal scheduler errors or Julia exiting), or
until all tasks in the graph have completed executing and the final task in the
graph is ready to be returned to the user. This outer loop continuously
performs two main operations: the first is to launch the execution of nodes
which have become "ready" to execute; the second is to "finish" nodes which
have been completed.

## Core: Initialization

At the very beginning of a scheduler's lifecycle, a `ComputeState` object is
allocated, workers are asynchronously initialized, and the outer loop is
started. Additionally, the scheduler is passed one or more tasks to start
scheduling, and so it will also fill out the `ComputeState` with the computed
sets of dependencies between tasks, initially placing all tasks are placed in
the "waiting" state. If any of the tasks are found to only have non-task input
arguments, then they are considered ready to execute and moved from the
"waiting" state to "ready".

## Core: Outer Loop

At each outer loop iteration, all tasks in the "ready" state will be scheduled,
moved into the "running" state, and asynchronously sent to the workers for
execution (called "firing"). Once all tasks are either waiting or running, the
scheduler may sleep until actions need to be performed

When fired tasks have completed executing, an entry will exist in the inbound
queue signalling the task's result and other metadata. At this point, the most
recently-queued task is removed from the queue, "finished", and placed in the
"finished" state. Finishing usually unlocks downstream tasks from the waiting
state and allows them to transition to the ready state.

## Core: Task Scheduling

Once one or more tasks are ready to be scheduled, the scheduler will begin assigning them to the processors within each available worker. This is a sequential operation consisting of:

- Selecting candidate processors based on the task's combined scope
- Calculating the cost to move needed data to each candidate processor
- Adding a "wait time" cost proportional to the estimated run time for all the tasks currently executing on each candidate processor
- Selecting the least costly candidate processor as the executor for this task

After these operations have been performed for each task, the tasks will be
fired off to their appropriate worker for handling.

## Worker: Task Execution

Once a worker receives one or more tasks to be executed, the tasks are
immediately enqueued into the appropriate processor's queue, and the processors
are notified that work is available to be executed. The processors will
asynchronously look at their queues and pick the task with the lowest occupancy
first; a task with zero occupancy will always be executed immediately, but most
tasks have non-zero occupancy, and so will be executed in order of increasing
occupancy (effectively prioritizing asynchronous tasks like I/O).

Before a task begins executions, the processor will collect the task's
arguments from other workers as needed, and convert them as needed to execute
correctly according to the processor's semantics. This operation is called a
"move".

Once a task's arguments have been moved, the task's function will be called
with the arguments, and assuming the task doesn't throw an error, the result
will be wrapped in a `Chunk` object. This `Chunk` will then be sent back to the
core scheduler along with information about which task generated it. If the
task does throw an error, then the error is instead propagated to the core
scheduler, along with a flag indicating that the task failed.

## Worker: Workload Balancing

In general, Dagger's core scheduler tries to balance workloads as much as
possible across all the available processors, but it can fail to do so
effectively when either its cached knowledge of each worker's status is
outdated, or when its estimates about the task's behavior are inaccurate. To
minimize the possibility of workload imbalance, the worker schedulers'
processors will attempt to steal tasks from each other when they are
under-occupied. Tasks will only be stolen if the task's [scope](scopes.md) is
compatible with the processor attempting the steal, so tasks with wider scopes
have better balancing potential.

## Core: Finishing

Finishing a task which has completed executing is generally a simple set of operations:

- The task's result is registered in the `ComputeState` for any tasks or user code which will need it
- Any unneeded data is cleared from the scheduler (such as preserved `Chunk` arguments)
- Downstream dependencies will be moved from "waiting" to "ready" if this task was the last upstream dependency to them

## Core: Shutdown

If the core scheduler needs to shutdown due to an error or Julia exiting, then
all workers will be shutdown, and the scheduler will close any open channels.
If shutdown was due to an error, then an error will be printed or thrown back
to the caller.
