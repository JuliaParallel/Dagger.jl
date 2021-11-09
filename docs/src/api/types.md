```@meta
CurrentModule = Dagger
```

# Types
```@index
Pages = ["types.md"]
```

## General
```@docs
Thunk
EagerThunk
Chunk
UnitDomain
```

## Tables
```@docs
DTable
GDTable
```

## Arrays
```@docs
DArray
Blocks
ArrayDomain
```

## Processors
```@docs
Processor
OSProc
ThreadProc
```

## Context
```@docs
Context
```

## Logging
```@docs
NoOpLog
LocalEventLog
```

## Scheduling
```@docs
Sch.SchedulerOptions
Sch.ThunkOptions
Sch.MaxUtilization
Sch.DynamicThunkException
```

## File IO

!!! warning
    These APIs are currently untested and may be removed or modified.

```@docs
FileReader
```
