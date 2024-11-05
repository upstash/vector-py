# Upstash Vector Python SDK

> [!NOTE]  
> **This project is in GA Stage.**
>
> The Upstash Professional Support fully covers this project. It receives regular updates, and bug fixes. 
> The Upstash team is committed to maintaining and improving its functionality.

## What is Upstash Vector?

Upstash Vector is a serverless vector database designed for managing and querying vector embeddings, ideal for AI and personalized data applications. It handles numeric representations of various objects (images, text, etc.) in multi-dimensional space, enabling similarity-based querying for tailored insights.

### Core Features

- **Serverless Architecture**: Operates without infrastructure management, with cost-effective, usage-based billing based on API calls.
- **High-Performance Queries**: Utilizes DiskANN for fast, high-recall queries, outperforming traditional exhaustive search methods.
- **Similarity Functions**: Supports Euclidean distance, Cosine similarity, and Dot Product for flexible similarity search.
- **Metadata Support and Filtering**: Attach metadata to vectors for added context and apply metadata-based filters to refine search results.

For more details, see the [Upstash Vector documentation](https://upstash.com/docs/vector/overall/getstarted).

## Installation

Install a released version from pip:
```shell
pip3 install upstash-vector
```

## Quick Start

1. **Create a Vector Database**: Head to the [Upstash Console](https://console.upstash.com) to create a vector database and obtain your `UPSTASH_VECTOR_REST_URL` and `UPSTASH_VECTOR_REST_TOKEN`.

2. **Initialize the Index**:

```python
from upstash_vector import Index

index = Index(url="your_rest_url", token="your_rest_token")
```

Or use environment variables:

```bash
export UPSTASH_VECTOR_REST_URL=[URL]
export UPSTASH_VECTOR_REST_TOKEN=[TOKEN]
```

```python
from upstash_vector import Index
index = Index.from_env()
```

3. **Upsert and Query Vectors**: Insert vectors into namespaces and perform similarity queries to retrieve relevant data.

```python
index.upsert(
    vectors=[
        # Upserting with tuple format
        ("id1", [0.1, 0.2], {"metadata_field": "metadata_value"}, "data-value"),
        ("id2", [0.2, 0.3], {"metadata_field": "another_value"}),  # Without data
        ("id3", [0.3, 0.4]),  # Without metadata and data

        # Upserting with dictionary format
        {"id": "id4", "vector": [0.4, 0.5], "metadata": {"field": "value"}, "data": "data-value"},
        {"id": "id5", "vector": [0.5, 0.6], "metadata": {"field": "another_value"}},  # Without data
        {"id": "id6", "vector": [0.6, 0.7]},  # Without metadata and data

        # Upserting with Vector objects
        Vector(id="id7", vector=[0.7, 0.8], metadata={"field": "value"}, data="text-data"),
        Vector(id="id8", vector=[0.8, 0.9], metadata={"field": "another_value"}),
        Vector(id="id9", vector=[0.9, 1.0])
    ]
)

# Query vectors that are most similar to a specified query vector
res = index.query(
    vector=[0.6, 0.9],  # Query vector
    top_k=5,  # Number of closest matches to return
    include_vectors=False,  # Whether to include actual vector data in results
    include_metadata=True,  # Whether to include metadata in results
    include_data=True,  # Whether to include data in results
    filter="metadata_field = 'metadata_value'"  # Optional filter based on metadata
)

# Iterate over and print the query results
for r in res:
    print(
        f"ID: {r.id}",  # Unique identifier for the vector
        f"Score: {r.score}",  # Similarity score to the query vector
        f"Vector: {r.vector if r.vector else 'N/A'}",  # The vector data, if included
        f"Metadata: {r.metadata if r.metadata else 'N/A'}",  # Metadata associated with the vector
        f"Data: {r.data if r.data else 'N/A'}"  # Additional data, if included
    )
```

## Docs
For full usage details, including advanced options and examples, refer to the [Upstash Vector documentation](https://upstash.com/docs/vector/overall/getstarted).

## Contributing

### Setup

This project uses [Poetry](https://python-poetry.org) for packaging and dependencies. After cloning the repository, install dependencies with:

```shell
poetry install
```

You will also need a vector database on [Upstash](https://console.upstash.com/).

### Code Formatting
```bash 
poetry run ruff format .
```

### Running Tests

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
