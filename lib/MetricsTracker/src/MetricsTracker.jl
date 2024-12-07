module MetricsTracker

import MacroTools: @capture
import ScopedValues: ScopedValue, @with
import TaskLocalValues: TaskLocalValue

include("types.jl")
include("metrics.jl")
include("lookup.jl")
include("io.jl")
include("builtins.jl")
# FIXME
#include("analysis.jl")
#include("aggregate.jl")
#include("decision.jl")

end # module MetricsTracker
