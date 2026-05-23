# How it Works

`pydbzengine` provides a seamless bridge between Python and the Java-based Debezium Engine.

## Architecture

At its core, `pydbzengine` leverages [JPype](https://jpype.readthedocs.io/en/stable/) to:
1.  **Launch a JVM**: A Java Virtual Machine is started within the Python process.
2.  **Load Debezium Jars**: The library bundles the necessary Debezium Engine and connector JAR files.
3.  **Proxy Objects**: It wraps Java objects and types in Pythonic interfaces, allowing you to use `Properties` and `DebeziumJsonEngine` as if they were native Python classes.

## Data Flow

1.  **Configuration**: You define your Debezium configuration using a standard Python dictionary.
2.  **Engine Initialization**: The `DebeziumJsonEngine` is initialized with these properties and a Python-based handler.
3.  **Event Capture**: The Java Debezium Engine captures CDC events from your source database.
4.  **Batch Processing**: Instead of processing events one by one in Java, `pydbzengine` passes batches of events to your Python handler's `handleJsonBatch` method.
5.  **Python Logic**: Your custom logic (or built-in handlers like Iceberg/dlt) processes the `ChangeEvent` objects in pure Python.

## Performance Considerations

By processing events in batches and leveraging Arrow/Parquet internally for handlers like Iceberg, `pydbzengine` maintains high throughput while providing the flexibility of Python.

---

## JVM & Troubleshooting

Because `pydbzengine` operates by running a Java Virtual Machine (JVM) inside the Python process using JPype, you may run into JVM-specific initialization errors. Here is how to resolve them:

### 1. `JVMNotSupportedException` or Cannot Locate JVM
If JPype cannot automatically find your Java installation, you will see errors during engine startup.

*   **Solution**: Ensure that **JDK 17 or newer** is installed.
*   **Environment Variable**: Set the `JAVA_HOME` environment variable pointing to your JDK home directory:
    ```shell
    # macOS example (add to ~/.zshrc or ~/.bash_profile)
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    
    # Linux example
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    ```

### 2. `JVMAlreadyStartedException`
JPype starting restriction means the JVM can **only be initialized once** per Python process. If another library starts the JVM, or if you run multiple tests that independently initialize JVM setups, JPype will throw a JVM already started warning or error.

*   **Solution**: The initialization module in `pydbzengine._jvm` checks `jpype.isJVMStarted()` before startup. However, if you are writing custom JVM startup scripts, always wrap it:
    ```python
    import jpype
    if not jpype.isJVMStarted():
        jpype.startJVM(...)
    ```

### 3. ClassNotFoundError (Debezium classes not found)
If the Debezium engine fails with Java class definition errors, the `.jar` dependencies might be missing or corrupted.

*   **Solution**: Re-run the installation of the package or, if developing locally, execute:
    ```shell
    ./pydbzengine/install_libs.sh
    ```
    This script downloads and copies all required connector jar files from Maven into the library directory `pydbzengine/debezium/libs`.
