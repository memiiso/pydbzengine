# Contributing

## Development Setup

1.  **Fork and Clone the Repository**:
    ```shell
    git clone https://github.com/your-username/pydbzengine.git
    cd pydbzengine
    ```
2.  **Install Development Dependencies**:
    ```shell
    pip install -e ".[dev]"
    ```

## Verification & Testing

1.  **Lint and Type Check**:
    ```shell
    ruff check .
    mypy pydbzengine
    ```
2.  **Run Unit Tests** (no Docker required):
    ```shell
    python -m unittest discover -s tests/unit
    ```
3.  **Run Integration Tests**:

    !!! important "Docker Prerequisite"
        The integration test suite (`tests/integration/`) uses `testcontainers` to start PostgreSQL, MySQL, MariaDB, MongoDB, Apache Iceberg REST catalog, and MinIO containers. Docker must be running before executing integration tests.

    ```shell
    python -m unittest discover -s tests/integration
    ```

## Documentation Preview

1.  Install documentation dependencies:
    ```shell
    pip install -e ".[docs]"
    ```
2.  Serve the documentation locally at `http://127.0.0.1:8000`:
    ```shell
    mkdocs serve
    ```
