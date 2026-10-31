# How it Works

`pydbzengine` bridges the Java-based Debezium Embedded Engine into Python using [JPype](https://jpype.readthedocs.io/en/stable/).

## Architecture

1.  **JVM Initialization**: Starts an embedded JVM inside the Python process when the engine is initialized.
2.  **Bundled Connector JARs**: Loads Debezium Engine and database connector JARs from `pydbzengine/debezium/libs`.
3.  **Callback Bridge**: Wraps Java `DebeziumEngine.ChangeConsumer` batches and forwards them to Python handler classes (`BasePythonChangeHandler` and `SchemaAwareChangeHandler`).

## Data Flow

1.  **Configuration**: Define Debezium connector properties in a Python `dict`.
2.  **Engine Initialization**: Pass the properties, a handler instance, and the wire format (`"json"` or `"connect"`) to `DebeziumEngine`.
3.  **Event Capture**: The Java Debezium Engine reads change events from the source database transaction log.
4.  **Batch Dispatch**: The JPype consumer forwards each batch of `ChangeEvent` records to the Python handler (`handle_batch` / `handleJsonBatch`).
5.  **Schema Conversion & Sink Processing**:
    - If using [`SchemaAwareChangeHandler`](schema/consumer.md), `EventConverter` compiles the event schemas into `TableSchema` descriptors, slices records into homogeneous `TableBatch` runs, and invokes `on_table_created`, `on_schema_evolved`, and `handle_table_batch`.
    - If using [`BasePythonChangeHandler`](handlers/custom.md), the handler processes the `list[ChangeEvent]` batch directly.

---

## JVM Troubleshooting

### `JVMNotSupportedException` or Cannot Locate JVM

If JPype cannot locate a Java installation during startup:

*   Verify that **JDK 17 or newer** is installed.
*   Set the `JAVA_HOME` environment variable to point to your JDK directory:
    ```shell
    # macOS
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)

    # Linux
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    ```

### `JVMAlreadyStartedException`

JPype allows the JVM to be started only once per Python process. `pydbzengine.engine.jvm.ensure_jvm_started()` checks `jpype.isJVMStarted()` before starting the JVM. If your application starts JPype directly, guard the call:

```python
import jpype

if not jpype.isJVMStarted():
    jpype.startJVM(...)
```

### `ClassNotFoundException` (Debezium Classes Not Found)

If the engine fails with missing Java class definitions during local development, download the required Maven JAR dependencies into `pydbzengine/debezium/libs`:

```shell
./pydbzengine/install_libs.sh
```
