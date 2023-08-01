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
delayed
@par
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
@mutable
@shard
shard
```

## Scope Functions
```@docs
scope
constrain
```

## Lazy Task Functions
```@docs
domain
compute
dependents
noffspring
order
treereduce
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

## Thunk Execution Environment Functions

These functions are used within the function called by a `Thunk`.

```@docs
in_thunk
thunk_processor
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
