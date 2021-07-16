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

## Arrays
```@docs
DArray
Blocks
ArrayDomain
```

## File IO

!!! warning
    These APIs are currently untested and may be removed or modified.

```@docs
FileReader
```
