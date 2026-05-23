# Contributing

Contributions are welcome! Whether it's reporting a bug, improving documentation, or adding new features, your help is appreciated.

## Getting Started

1.  **Fork the Repository**: Create a fork of `memiiso/pydbzengine` on GitHub.
2.  **Clone Locally**:
    ```shell
    git clone https://github.com/your-username/pydbzengine.git
    cd pydbzengine
    ```
3.  **Install Development Dependencies**:
    ```shell
    pip install -e ".[dev]"
    ```

## Development Workflow

1.  **Create a Branch**: Use a descriptive name for your feature or bug fix.
2.  **Make Changes**: Write your code and tests.
3.  **Run Tests**:
    
    > [!IMPORTANT]
    > **Docker Prerequisite**: The integration test suite uses `testcontainers` to spin up real PostgreSQL database, Apache Iceberg REST catalog, and MinIO storage instances. Docker must be running on your machine to execute the tests.
    
    To run the entire test suite:
    ```shell
    pytest
    ```
    
    To execute a specific test file (e.g., Iceberg V2 handlers):
    ```shell
    pytest tests/test_iceberg_handlerv2.py
    ```
4.  **Update Documentation**: If you're adding a new feature or changing behavior, update the relevant `docs/` files.
5.  **Submit a Pull Request**: Push your branch to your fork and create a PR against the `main` branch.

## Documentation Development

To preview the documentation locally:

1.  Install documentation dependencies:
    ```shell
    pip install -e ".[docs]"
    ```
2.  Serve the documentation:
    ```shell
    mkdocs serve
    ```
3.  Open `http://127.0.0.1:8000` in your browser.
