```@meta
CurrentModule = Dagger
```

# Functions
```@index
Pages = ["functions.md"]
```

## General
```@docs
delayed
spawn
tochunk
domain
compute
dependents
noffspring
order
treereduce
```

## Tables
```@docs
tabletype
tabletype!
map
reduce
filter
groupby
```

## Arrays
```@docs
alignfirst
view
```

## Processors
```@docs
execute!
iscompatible
default_enabled
get_processors
get_parent
move
capacity
get_tls
set_tls!
```

## Context
```@docs
addprocs!
rmprocs!
```

## Logging
```@docs
get_logs!
```

## Thunk Execution Environment

These functions are used within the function called by a `Thunk`.

```@docs
in_thunk
thunk_processor
```

### Dynamic Scheduler Control

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

## File IO

!!! warning
    These APIs are currently untested and may be removed or modified.

```@docs
save
load
```

# Macros
```@docs
@par
@spawn
```
