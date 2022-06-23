```@meta
CurrentModule = Dagger
```

# Types
```@index
Pages = ["types.md"]
```

## General Types
```@docs
Thunk
EagerThunk
Chunk
UnitDomain
```

## Array Types
```@docs
DArray
Blocks
ArrayDomain
```

## Processor Types
```@docs
Processor
OSProc
ThreadProc
```

## Scope Types
```@docs
AnyScope
NodeScope
ProcessScope
UnionScope
ExactScope
```

## Context Types
```@docs
Context
```

## Logging Types
```@docs
NoOpLog
LocalEventLog
```

## Scheduling Types
```@docs
Sch.SchedulerOptions
Sch.ThunkOptions
Sch.MaxUtilization
Sch.DynamicThunkException
```

## File IO Types

!!! warning
    These APIs are currently untested and may be removed or modified.

```@docs
FileReader
```
