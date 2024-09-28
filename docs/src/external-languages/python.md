# Python Integration

If you're using Python as your main programming language, Dagger can be easily
integrated into your workflow. Dagger has built-in support for Python, which
you can easily access through the `pydaggerjl` library (accessible through
`pip`). This library provides a Pythonic interface to Dagger, allowing you to
spawn Dagger tasks that run Python functions on Python arguments.

Here's a simple example of interfacing between Python's `numpy` library and
Dagger:

```python
import numpy as np
from pydaggerjl import daggerjl

# Create a Dagger DTask to sum the elements of an array
task = daggerjl.spawn(np.sum, np.array([1, 2, 3]))

# Wait on task to finish
# This is purely educational, as fetch will wait for the task to finish
daggerjl.wait(task)

# Fetch the result
result = daggerjl.fetch(task)

print(f"The sum is: {result}")

# Create two numpy arrays
a = np.array([1, 2, 3])
b = np.array([4, 5, 6])

# Element-wise sum of the two arrays
task = daggerjl.spawn(np.add, a, b)

# Fetch the result
result = daggerjl.fetch(task)

print(f"The element-wise sum is: {result}")

# Element-wise sum of last result with itself
task2 = daggerjl.spawn(np.add, task, task)

# Fetch the result
result2 = daggerjl.fetch(task2)

print(f"The element-wise sum of the last result with itself is: {result2}")
```

Keep an eye on Dagger and pydaggerjl - new features are soon to come!
