# Logging and Graphing

Dagger's scheduler keeps track of the important and potentially expensive
actions it does, such as moving data between workers or executing thunks, and
tracks how much time and memory allocations these operations consume, among
other things. It does it through the `TimespanLogging.jl` package (which used
to be directly integrated into Dagger). Saving this information somewhere
accessible is disabled by default, but it's quite easy to turn it on, through
two mechanisms.

The first is `Dagger.enable_logging!`, which provides an easy-to-use interface
to both enable and configure logging. The defaults are usually sufficient for
most users, but can be tweaked with keyword arguments.

The second is done by setting a "log sink" in the Dagger `Context` being used,
as `ctx.log_sink`. These log sinks drive how Dagger's logging behaves, and are
configurable by the user, without the need to tweak any of Dagger's internal
code.

A variety of log sinks are built-in to TimespanLogging; the `NoOpLog` is the
default log sink when one isn't explicitly specified, and disables logging
entirely (to minimize overhead). There are currently two other log sinks of
interest; the first and newer of the two is the `MultiEventLog`, which
generates multiple independent log streams, one per "consumer" (details in the
next section). This is the log sink that `enable_logging!` uses, as it's easily
the most flexible. The second and older sink is the `LocalEventLog`, which is
explained later in this document. Most users are recommended to use the
`MultiEventLog` (ideally via `enable_logging!`) since it's far more flexible
and extensible, and is more performant in general.

Log sinks are explained in detail in [Logging: Advanced](@ref); however, if
using `enable_logging!`, everything is already configured for you. Then, all
you need to do is call `Dagger.fetch_logs!()` to get the logs for all workers
as a `Dict`. A variety of tools can operate on these logs, including
visualization through `show_logs` and `render_logs`.
