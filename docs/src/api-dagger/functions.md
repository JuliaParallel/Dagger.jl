```@meta
CurrentModule = Dagger
```

# Dagger Functions
```@index
Pages = ["functions.md"]
```

## Task Functions/Macros
```@docs
@spawn
spawn
```

## Task Options Functions/Macros
```@docs
with_options
get_options
@option
default_option
```

## Data Management Functions
```@docs
tochunk
mutable
@mutable
@shard
shard
```

## Data Dependencies Functions
```@docs
spawn_datadeps
```

## Scope Functions
```@docs
scope
constrain
```

## Processor Functions
```@docs
execute!
iscompatible
default_enabled
get_processors
get_parent
move
get_tls
set_tls!
```

## Context Functions
```@docs
addprocs!
rmprocs!
```

## DTask Execution Environment Functions

These functions are used within the function called by a `DTask`.

```@docs
in_task
task_processor
```

### Dynamic Scheduler Control Functions

These functions query and control the scheduler remotely.

```@docs
Sch.sch_handle
Sch.add_thunk!
Base.fetch
Base.wait
Sch.exec!
Sch.halt!
Sch.get_dag_ids
```
