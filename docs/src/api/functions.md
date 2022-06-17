```@meta
CurrentModule = Dagger
```

# Functions
```@index
Pages = ["functions.md"]
```

## General Functions
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

## Table Functions
```@docs
tabletype
tabletype!
trim
trim!
map
filter
reduce
mapreduce
groupby
leftjoin
innerjoin
keys
getindex
```

## Array Functions
```@docs
alignfirst
view
```

## Processor Functions
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

## Shard Functions
[`Dagger.@shard`](@ref)
[`Dagger.shard`](@ref)

## Context Functions
```@docs
addprocs!
rmprocs!
```

## Logging Functions
```@docs
get_logs!
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

## File IO Functions

!!! warning
    These APIs are currently untested and may be removed or modified.

```@docs
save
load
```

# Macros API
```@docs
@par
@spawn
```
