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
