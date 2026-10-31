# Helper Utilities

`pydbzengine.helper.Utils` provides execution helpers to run `DebeziumEngine` with a time limit or stop after an initial snapshot completes.

## Methods Reference

### `Utils.run_engine_async`

Runs `engine.run()` in a daemon thread and calls `engine.close()` when `timeout_sec` elapses.

```python
@staticmethod
def run_engine_async(
    engine: DebeziumEngine, timeout_sec: int = 22, blocking: bool = True
)
```

*   **`engine`**: `DebeziumEngine` instance to execute.
*   **`timeout_sec`**: Maximum execution duration in seconds. Defaults to `22`.
*   **`blocking`**: If `True`, blocks the calling thread via `join()` until the engine stops or times out. Defaults to `True`.

```python
from pydbzengine import DebeziumEngine
from pydbzengine.helper import Utils

engine = DebeziumEngine(properties=props, handler=handler)
Utils.run_engine_async(engine=engine, timeout_sec=60)
```

---

### `Utils.run_engine_until_snapshot`

Runs `engine.run()` in a daemon thread and polls log output for `"Snapshot completed"`. Once detected, it calls `engine.close()` to shut down the engine. Use this helper with `snapshot.mode = initial_only` for batch snapshot jobs.

```python
@staticmethod
def run_engine_until_snapshot(engine: DebeziumEngine, poll_interval_sec: int = 1)
```

*   **`engine`**: `DebeziumEngine` instance to execute.
*   **`poll_interval_sec`**: Polling interval in seconds to check log output for snapshot completion. Defaults to `1`.

```python
from pydbzengine import DebeziumEngine
from pydbzengine.helper import Utils

# Properties include "snapshot.mode": "initial_only"
engine = DebeziumEngine(properties=props, handler=handler)
Utils.run_engine_until_snapshot(engine=engine)
```
