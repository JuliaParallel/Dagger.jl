# Scheduler Internals

The scheduler is called `Dagger.Sch`. It contains a single internal instance
of type `ComputeState`, which maintains all necessary state to represent the
set of waiting, ready, and completed (or "finished") graph nodes, cached
`Chunk`s, and maps of interdependencies between nodes. It uses Julia's task
infrastructure to asynchronously send work requests to remote compute
processes, and uses a Julia `Channel` as an inbound queue for completed work.
There is an outer loop which drives the scheduler, which continues executing
until all nodes in the graph have completed executing and the final result of
the graph is ready to be returned to the user. This outer loop continuously
performs two main operations: the first is to launch the execution of nodes
which have become "ready" to execute; the second is to "finish" nodes which
have been completed.

## Scheduler Initialization

At the very beginning of a scheduler's lifecycle, the `ComputeState` is
elaborated based on the computed sets of dependencies between nodes, and all
nodes are placed in a "waiting" state. If any of the nodes are found to only
have inputs which are not `Thunk`s, then they are moved from "waiting" to
"ready". The set of available "workers" (the set of available compute
processes located throughout the cluster) is recorded, of size `Nworkers`.

## Scheduler Outer Loop

At each outer loop iteration, up to `Nworkers` processes that are currently in
the "ready" state will be moved into the "running" state, and asynchronously
sent (along with input arguments) to one of the `Nworkers` processes for
execution. Subsequently, if any nodes exist in the inbound queue (i.e. the
nodes have completed execution and their result is stored on the process that
executed the node), then the most recently-queued node is removed from the
queue, "finished", and placed in the "finished" state.

## Node Execution

Executing a node (here called `Ne`) in the "ready" state comprises two tasks.
The first task is to identify which node in the set of "ready" nodes will be
`Ne` (the node to execute). This choice is based on a concept known as
"affinity", which is a cost-based metric used to evaluate the suitability of
executing a given node on a given process. The metric is based primarily on
the location of the input arguments to the node, as well as the arguments
computed size in bytes. A fixed amount of affinity is added for each argument
when the process in question houses that argument. Affinity is then added
based on some base affinity value multiplied by the argument's size in bytes.
The total affinities for each node are then used to pick the most optimal node
to execute (typically, the one with the highest affinity).

The second task is to prepare and send the node to a process for execution. If
the node has been executed in the past (due to it being an argument to
multiple other nodes), then the node is finished, and its result is pulled
from the cache. If the node has not yet been executed, it is first checked if
it is a "meta" node. A "meta" node is explicitly designated as such by the
user or library, and will execute directly on its inputs as chunks (the data
contained in the chunks are not immediately retrieved from the processors they
reside on). Such a node will be executed directly within the scheduler, under
the assumption that such a node is not expensive to execute. If the node is
not a "meta" node, the executing worker process chooses (in round-robin
fashion) a suitable processor to execute to execute the node on, based on the
node's function, the input argument types, and user-defined rules for
processor selection. The input arguments are then asynchronously transferred
(via processor move operation) to the selected processor, and the appropriate
call to the processor is made with the function and input arguments. Once
execution completes and a result is obtained, it is wrapped as a `Chunk`, and
the `Chunk`'s handle is returned to the scheduler's inbound queue for node
finishing.

## Node Finishing

"Finishing" a node (here called `Nf`) performs three main tasks. The first
task is to find all of the downstream "children" nodes of `Nf` (the set of
nodes which use `Nf`'s result as one of their input arguments) that have had
all of their input arguments computed and are in the "waiting" state, and move
them into the "ready" state. The second task is to check all of the inputs to
`Nf` to determine if any of them no longer have children nodes which have not
been finished; if such inputs match this pattern, their cached result may be
freed by the scheduler to minimize data usage. The third task is to mark `Nf`
as "finished", and also to indicate to the scheduler whether another node has
become "ready" to execute.
