# Custom Handlers

You can easily create your own custom handlers by extending the `BasePythonChangeHandler` class.

## Creating a Handler

You only need to implement the `handleJsonBatch` method. This method receives a list of `ChangeEvent` objects.

```python
from typing import List
from pydbzengine import ChangeEvent, BasePythonChangeHandler

class MyCustomHandler(BasePythonChangeHandler):
    def handleJsonBatch(self, records: List[ChangeEvent]):
        print(f"Received batch of {len(records)} records")
        for record in records:
            # Access event data
            dest = record.destination()
            key = record.key()
            value = record.value()
            
            # Implement your logic here (e.g., push to Kafka, call API, etc.)
            print(f"Processing event for {dest}")
```

## The ChangeEvent Object

The `ChangeEvent` object provides the following methods:

- `destination()`: Returns the destination topic/table name.
- `key()`: Returns the JSON string representing the event key.
- `value()`: Returns the JSON string representing the event value.
