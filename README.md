# Upstash Vector Python SDK
The Upstash Vector python client

## Installation

Install a released version from pip:
```shell
pip3 install upstash-vector
```

## Usage
In order to use this client, head out to [Upstash Console](https://console.upstash.com) and create a vector database. There, get the URL and the TOKEN from the dashboard.

### Initialize the client
```python
from upstash_vector import Index

index = Index(url=UPSTASH_VECTOR_REST_URL, token=UPSTASH_VECTOR_REST_TOKEN)
```

or alternatively, initialize from the environment

```bash
export UPSTASH_VECTOR_REST_URL [URL]
export UPSTASH_VECTOR_REST_TOKEN [TOKEN]
```

```python
from upstash_vector import Index

index = Index.from_env()
```

### Upsert Vectors
There are couple ways to upsert vectors. Feel free to use whichever one feels the most comfortable.

```python
index.upsert(
    vectors=[
        ("id1", [0.1, 0.2], {"metadata_field": "metadata_value"}),
        ("id2", [0.3, 0.4]),
    ]
)
```

```python
index.upsert(
    vectors=[
        {"id": "id3", "vector": [0.1, 0.2], "metadata": {"metadata_f": "metadata_v"}},
        {"id": "id4", "vector": [0.5, 0.6]},
    ]
)
```

```python
from upstash_vector import Vector

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
# query_res is a list of vectors with scores:
# query_res[n].id: The identifier associated with the matching vector.
# query_res[n].score: A measure of similarity indicating how closely the vector matches the query vector.
# query_res[n].vector: The vector itself (included only if `include_vector` is set to `True`).
# query_res[n].metadata: Additional information or attributes linked to the matching vector.
```

### Fetch Indexes
```python
res = index.fetch(["id3", "id4"], include_vectors=True, include_metadata=True)
# res.vectors: A list containing information for each fetched vector, including `id`, `vector`, and `metadata`.
```

or, for singular fetch:

```python
res = index.fetch("id1", include_vectors=True, include_metadata=True)
```

### Range over Vectors - Scan the Index
```python
# Scans the index 3 by 3, until all the indexes are traversed.
res = index.range(cursor="", limit=3, include_vectors=True, include_metadata=True)
while res.next_cursor != "":
    res = index.range(cursor=res.next_cursor, limit=3, include_vectors=True, include_metadata=True)

# res.nex_cursor: A cursor indicating the position to start the next range query. If "", there are no more results.
# res.vectors: A list containing information for each vector, including `id`, `vector`, and `metadata`.
```

### Delete Vectors
```python
res = index.delete(["id1", "id2"])
# res.deleted: An integer indicating how many vectors were deleted with the command.
```

or, for singular deletion:

```python
res = index.delete("id1")
```

### Reset the Index
```python
# This will remove all the vectors that were upserted and index will be reset.
index.reset() 
```

### Index Stats
```python
stats = index.stats()
# stats.vector_count: total number of vectors in the index
# stats.pending_vector_count: total number of vectors waiting to be indexed
# stats.index_size: total size of the index on disk in bytes 
```

# Contributing

## Preparing the environment
This project uses [Poetry](https://python-poetry.org) for packaging and dependency management. Make sure you are able to create the poetry shell with relevant dependencies.

You will also need a vector database on [Upstash](https://console.upstash.com/).

```commandline
poetry install 
```

## Code Formatting
```bash 
poetry run ruff format .
```

## Running tests
To run all the tests, make sure the poetry virtual environment activated with all 
the necessary dependencies. Set the necessary environment variables and run:

```bash
poetry run pytest
```
