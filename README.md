# Upstash Vector Python SDK
The Upstash Vector python client

## Installation

Install a released version from pip:
```shell
pip3 install upstash-vector
```

## Usage
In order to use this client, head out to [Upstash Console](https://console.upstash.com) and create a vector database. There, get the URL and the token from the dashboard.

### Initialize the client
```python
from upstash_vector import Index

index = Index(url=URL, token=TOKEN)
```

### Upsert Vectors
There are couple ways to upsert vectors. Feel free to use whichever one feels the most comfortable.

```python
from upstash_vector import Index
index = Index(url=URL, token=TOKEN)

index.upsert(
    vectors=[
        ("id1", [0.1, 0.2], {"metadata_field": "metadata_value"}),
        ("id2", [0.3, 0.4]),
    ]
)
```

```python
from upstash_vector import Index
index = Index(url=URL, token=TOKEN)

index.upsert(
    vectors=[
        {"id": "id3", "vector": [0.1, 0.2], "metadata": {"metadata_f": "metadata_v"}},
        {"id": "id4", "vector": [0.5, 0.6]},
    ]
)
```
```python
from upstash_vector import Index
from upstash_vector import Vector
index = Index(url=URL, token=TOKEN)

index.upsert(
    vectors=[
        Vector(id="id5", vector=[1, 2], metadata={"metadata_f": "metadata_v"}),
        Vector(id="id6", vector=[6, 7]),
    ]
)
```

### Query Index
```python
query_vector = [0.6, 0.9]
top_k = 6
query_res = index.query(
    vector=query_vector,
    top_k=top_k,
    include_vectors=True,
    include_metadata=True,
)
```

### Fetch Indexes
```python
res = index.fetch(["id3", "id4"], include_vectors=True, include_metadata=True)
```

### Range over Vectors - Scan the Index
```python
# Scans the index 3 by 3, until all the indexes are traversed.
res = index.range(cursor="", limit=3, include_vectors=True, include_metadata=True)
while res.next_cursor != "":
    res = index.range(cursor=res.next_cursor, limit=3, include_vectors=True, include_metadata=True)
```

### Delete Vectors
```python
res = index.delete(["id1", "id2"])
```

### Reset the Index
```python
# This will remove all the vectors that were upserted and index will be reset.
index.reset() 
```

# Contributing

## Preparing the environment
This project uses [Poetry](https://python-poetry.org) for packaging and dependency management. Make sure you are able to create the poetry shell with relevant dependencies.

You will also need a vector database on [Upstash](https://console.upstash.com/).

```commandline
poetry install 
```

## Code Formatting
```ruff .```

## Running tests
To run all the tests, make sure the poetry virtual environment activated with all 
the necessary dependencies. Set the necessary environment variables and run:

```bash
poetry run pytest
```
