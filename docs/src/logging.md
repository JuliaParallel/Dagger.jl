# Logging

Dagger provides mechanisms to log and visualize scheduler events. This can be useful for debugging and performance analysis.

## Basic Logging Functions

The primary functions for controlling logging are:

- `Dagger.enable_logging!`: Enables logging. This function uses the `MultiEventLog` by default, which is flexible and performant. You can customize its behavior with keyword arguments.
- `Dagger.disable_logging!`: Disables logging.
- `Dagger.fetch_logs!`: Fetches the logs from all workers. This returns a `Dict` where keys are worker IDs and values are the logs.

## Example Usage

```julia
using Dagger

# Enable logging
Dagger.enable_logging!()

# Run some Dagger computations
wait(Dagger.@spawn sum([1, 2, 3]))

# Fetch logs
logs = Dagger.fetch_logs!()

# Disable logging
Dagger.disable_logging!()

# You can now inspect the `logs` Dict or use visualization tools
# like `show_logs` and `render_logs` (see [Logging: Visualization](@ref logging-visualization.md)).
```

For more advanced logging configurations, such as custom log sinks and consumers, see [Logging: Advanced](@ref).
