# Upstash Vector Python SDK
The Upstash Vector Python client

> [!NOTE]  
> **This project is in GA Stage.**
>
> The Upstash Professional Support fully covers this project. It receives regular updates, and bug fixes. 
> The Upstash team is committed to maintaining and improving its functionality.

## Installation

Install a released version from pip:
```shell
pip3 install upstash-vector
```

## Usage
In order to use this client, head out to [Upstash Console](https://console.upstash.com) and create a vector database. 
There, get the `UPSTASH_VECTOR_REST_URL` and the `UPSTASH_VECTOR_REST_TOKEN` from the dashboard.

### Initializing the Index
```python
from upstash_vector import Index

index = Index(url=UPSTASH_VECTOR_REST_URL, token=UPSTASH_VECTOR_REST_TOKEN)
```

or alternatively, initialize from the environment variables

```bash
export UPSTASH_VECTOR_REST_URL [URL]
export UPSTASH_VECTOR_REST_TOKEN [TOKEN]
```

```python
from upstash_vector import Index

index = Index.from_env()
```

### Upsert Vectors

Vectors can be upserted(inserted or updated) into a namespace of an index
to be later queried or fetched.

There are a couple of ways of doing upserts:

```python
# as tuples, either of the form: 
# - (id, vector, metadata, data)
# - (id, vector, metadata)
# - (id, vector) 

index.upsert(
    vectors=[
        ("id1", [0.1, 0.2], {"metadata_field": "metadata_value"}, "data-value"),
        ("id2", [0.2, 0.2], {"metadata_field": "metadata_value"}),
        ("id3", [0.3, 0.4]),
    ]
)
```

```python
# as dicts, either of the form:
# - {"id": id, "vector": vector, "metadata": metadata, "data" data)
# - {"id": id, "vector": vector, "metadata": metadata)
# - {"id": id, "vector": vector, "data": data)
# - {"id": id, "vector": vector} 

index.upsert(
    vectors=[
        {"id": "id4", "vector": [0.1, 0.2], "metadata": {"field": "value"}, "data": "value"},
        {"id": "id5", "vector": [0.1, 0.2], "metadata": {"field": "value"}},
        {"id": "id6", "vector": [0.1, 0.2], "data": "value"},
        {"id": "id7", "vector": [0.5, 0.6]},
    ]
)
```

```python
from upstash_vector import Vector

# as Vector objects

index.upsert(
    vectors=[
        Vector(id="id5", vector=[1, 2], metadata={"field": "value"}),
        Vector(id="id6", vector=[1, 2], data="value"),
        Vector(id="id7", vector=[6, 7]),
    ]
)
```

If the index is created with an embedding model, raw string data can be upserted.
In this case, the `data` field of the vector will also be set to the `data` passed
below, so that it can be accessed later.

```python
from upstash_vector import Data

res = index.upsert(
    vectors=[
        Data(id="id5", data="Goodbye World", metadata={"field": "value"}),
        Data(id="id6", data="Hello World"),
    ]
)
```

Also, a namespace can be specified to upsert vectors into it.
When no namespace is provided, the default namespace is used.

```python
index.upsert(
    vectors=[
        ("id1", [0.1, 0.2]),
        ("id2", [0.3,0.4]),
    ],
    namespace="ns",
)
```

### Query Vectors

Some number of vectors that are approximately most similar to a given
query vector can be requested from a namespace of an index.

```python
res = index.query(
    vector=[0.6, 0.9], 
    top_k=5,
    include_vectors=False,
    include_metadata=True,
    include_data=True,
    filter="metadata_f = 'metadata_v'"
)

# List of query results, sorted in the descending order of similarity
for r in res:
    print(
        r.id, # The id used while upserting the vector
        r.score, # The similarity score of this vector to the query vector. Higher is more similar.
        r.vector, # The value of the vector, if requested.
        r.metadata, # The metadata of the vector, if requested and present.
        r.data, # The data of the vector, if requested and present.
    )
```

If the index is created with an embedding model, raw string data can be queried.

```python
res = index.query(
    data="hello",
    top_k=5,
    include_vectors=False,
    include_metadata=True,
    include_data=True,
)
```

When a filter is provided, query results are further narrowed down based
on the vectors whose metadata matches with it.

See [Metadata Filtering](https://upstash.com/docs/vector/features/filtering) documentation 
for more information regarding the filter syntax. 

Also, a namespace can be specified to query from. 
When no namespace is provided,  the default namespace is used.

```python
res = index.query(
    vector=[0.6, 0.9], 
    top_k=5,
    namespace="ns",
)
```

### Fetch Vectors

A set of vectors can be fetched from a namespace of an index.

```python
res = index.fetch(
    ids=["id3", "id4"], 
    include_vectors=False, 
    include_metadata=True,
    include_data=True,
)

# List of fetch results, one for each id passed
for r in res:
    if not r: # Can be None, if there is no such vector with the given id
        continue
    
    print(
        r.id, # The id used while upserting the vector
        r.vector, # The value of the vector, if requested.
        r.metadata, # The metadata of the vector, if requested and present.
        r.data, # The metadata of the vector, if requested and present.
    )
```

or, for singular fetch:

```python
res = index.fetch(
    "id1", 
    include_vectors=True, 
    include_metadata=True,
    include_data=False,
)

r = res[0]
if r: # Can be None, if there is no such vector with the given id
    print(
        r.id, # The id used while upserting the vector
        r.vector, # The value of the vector, if requested.
        r.metadata, # The metadata of the vector, if requested and present.
        r.data, # The metadata of the vector, if requested and present.
    )
```

Also, a namespace can be specified to fetch from. 
When no namespace is provided, the default namespace is used.

```python
res = index.fetch(
    ids=["id3", "id4"], 
    namespace="ns",
)
```

### Range Over Vectors

The vectors upserted into a namespace of an index can be scanned
in a page by page fashion.

```python
# Scans the vectors 100 vector at a time,
res = index.range(
    cursor="", # Start the scan from the beginning 
    limit=100, 
    include_vectors=False, 
    include_metadata=True,
    include_data=True,
)

while res.next_cursor != "":
    res = index.range(
        cursor=res.next_cursor, 
        limit=100, 
        include_vectors=False, 
        include_metadata=True,
        include_data=True,
    )
    
    for v in res.vectors:
        print(
            v.id, # The id used while upserting the vector
            v.vector, # The value of the vector, if requested.
            v.metadata, # The metadata of the vector, if requested and present.
            v.data, # The data of the vector, if requested and present.
        )
```

Also, a namespace can be specified to range from. 
When no namespace is provided, the default namespace is used.

```python
res = index.range(
    cursor="", 
    limit=100,
    namespace="ns",
)
```

### Delete Vectors

A list of vectors can be deleted from a namespace of index.
If no such vectors with the given ids exist, this is no-op.

```python
res = index.delete(
    ids=["id1", "id2"],
)

print(
    res.deleted, # How many vectors are deleted out of the given ids.
)
```

or, for singular deletion:

```python
res = index.delete(
    "id1",
)

print(res) # A boolean indicating whether the vector is deleted or not.
```

Also, a namespace can be specified to delete from. 
When no namespace is provided, the default namespace is used.

```python
res = index.delete(
    ids=["id1", "id2"],
    namespace="ns",
)
```

### Update a Vector

Either the vector value(or data for indexes created with an embedding model) or the metadata
can be updated without needing to set the other one.

```python
res = index.update(
    "id1", 
    metadata={"new_field": "new_value"},
)

print(res) # A boolean indicating whether the vector is updated or not.
```

Also, a namespace can be specified to update from. 
When no namespace is provided, the default namespace is used.

```python
res = index.update(
    "id1", 
    metadata={"new_field": "new_value"},
    namespace="ns",
)
```

### Reset the Namespace

All vectors can be removed from a namespace of an index.

```python
index.reset() 
```

Also, a namespace can be specified to reset. 
When no namespace is provided, the default namespace is used.

```python
index.reset(
    namespace="ns",
) 
```

All namespaces under the index can be reset with a single call
as well.

```python
index.reset(
    all=True,
)
```

### Index Info

Some information regarding the status and type of the index can be requested.
This information also contains per-namespace status.

```python
info = index.info()
print(
    info.vector_count, # Total number of vectors across all namespaces
    info.pending_vector_count, # Total number of vectors waiting to be indexed across all namespaces
    info.index_size, # Total size of the index on disk in bytes
    info.dimension, # Vector dimension
    info.similarity_function, # Similarity function used
)

for ns, ns_info in info.namespaces.items():
    print(
        ns, # Name of the namespace
        ns_info.vector_count, # Total number of vectors in this namespaces
        ns_info.pending_vector_count, # Total number of vectors waiting to be indexed in this namespaces
    )
```

### List Namespaces

All the names of active namespaces can be listed.

```python
namespaces = index.list_namespaces()
for ns in namespaces:
    print(ns) # name of the namespace
```

### Delete a Namespace

A namespace can be deleted entirely. 
If no such namespace exists, and exception is raised.
The default namespaces cannot be deleted.

```python
index.delete_namespace(namespace="ns")
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
the necessary dependencies.

Create two Vector Stores on upstash. First one should have 2 dimensions. Second one should use an embedding model. Set the necessary environment variables:

```
URL=****
TOKEN=****
EMBEDDING_URL=****
EMBEDDING_TOKEN=****
```

Then, run the following command to run tests:

```bash
poetry run pytest
```
