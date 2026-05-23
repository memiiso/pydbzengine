# Helper Utilities

The `pydbzengine` library provides a set of helper functions inside the [Utils](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/helper.py#L20-L102) class (located in [helper.py](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/helper.py)) to help manage the engine's lifecycle, run the engine asynchronously, or run until snapshots complete.

## Methods Reference

### `Utils.run_engine_async`

Runs the given engine's `run` method in a separate background thread with a maximum duration (timeout). If the engine exceeds the timeout limit, it raises a `TimeoutError` and calls `engine.close()`.

```python
@staticmethod
def run_engine_async(engine: DebeziumJsonEngine, timeout_sec: int = 22, blocking: bool = True)
```

*   **`engine`**: The [DebeziumJsonEngine](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/__init__.py#L68) instance to execute.
*   **`timeout_sec`**: The maximum execution time in seconds. Defaults to `22`.
*   **`blocking`**: If `True`, blocks the calling thread (e.g. main thread) using `join()` until the engine completes or times out. Defaults to `True`.

#### Example Usage

```python
from pydbzengine.helper import Utils

# Run the engine for exactly 60 seconds and then stop
Utils.run_engine_async(engine=engine, timeout_sec=60)
```

---

### `Utils.run_engine_until_snapshot`

Runs the Debezium engine in a daemon thread and monitors logging outputs. Once the log message `"Snapshot completed"` is detected, the engine is shut down gracefully. This is extremely useful for loading initial tables/snapshots when using `snapshot.mode = initial_only` (or equivalent Debezium snapshot modes) in batched ingestion jobs.

```python
@staticmethod
def run_engine_until_snapshot(engine: DebeziumJsonEngine, poll_interval_sec: int = 1)
```

*   **`engine`**: The [DebeziumJsonEngine](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/__init__.py#L68) instance to execute.
*   **`poll_interval_sec`**: The frequency (in seconds) to check log handlers for snapshot completion. Defaults to `1`.

#### Example Usage

```python
from pydbzengine.helper import Utils

# Config properties include: "snapshot.mode": "initial_only"
Utils.run_engine_until_snapshot(engine=engine)
```
