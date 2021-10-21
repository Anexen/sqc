# SQC

SQL Queries to Collections

# Examples

```python
from sqc import sqc

data = [
    {"a": 1, "b": 1},
    {"a": 2, "b": 1},
    {"a": 3, "b": 2},
]

```

Simple filtering

```python
query = sqc("SELECT b WHERE a = 2")
reset = query.execute(data)
assert result == [{"b": 1}]
```

# Roadmap:

Named data source:

```python
query = sqc("SELECT b FROM table WHERE a < 2")
reset = query.execute(table=data)
assert result == [{"b": 1}]
```

Attribute access:

```python
from typing import NamedTuple

class Row(NamedTuple):
    a: int
    b: int

query = sqc("SELECT b WHERE a < 2", field_getter=getattr)
assert isinstance(query, SqcQuery)
reset = query.execute(map(Row._make, data))
assert result == [{"b": 1}]
```

Nested data structures:

```python
query = sqc("SELECT path(a, 'c[a].b[0]') AS q, c WHERE b > 1")
```

Custom output structure:

```python
query = sqc("SELECT b WHERE a < 2", output=tuple)
reset = query.execute(data)
assert result == [(1,)]
```

Shortcuts:

```python
query = sqc("SELECT b WHERE a < 2")
assert query.one(data) = (1,)
assert query.scalar(data) == 1
```
