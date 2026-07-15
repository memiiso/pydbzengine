from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from pydbzengine._jvm import Properties


class RecordCommitter(ABC):
    """
    Abstract base class for type hinting the RecordCommitter.
    Mimics the io.debezium.engine.DebeziumEngine$RecordCommitter interface for Python.
    """

    @abstractmethod
    def markProcessed(self, record):
        """Marks a single record as processed."""
        pass

    @abstractmethod
    def markBatchFinished(self):
        """Marks the entire batch as finished."""
        pass


class ChangeEvent(ABC):
    """
    Abstract base class for type hinting the ChangeEvent.
    Mimics the org.apache.kafka.connect.connector.ConnectRecord interface for Python.
    """

    @abstractmethod
    def key(self) -> str:
        """Returns the record key."""
        pass

    @abstractmethod
    def value(self) -> str:
        """Returns the record value (payload)."""
        pass

    @abstractmethod
    def destination(self) -> str:
        """Returns the destination topic/table."""
        pass

    @abstractmethod
    def partition(self) -> int:
        """Returns the partition the record belongs to."""
        pass


class BasePythonChangeHandler(ABC):
    """
    Abstract base class for user-defined change event handlers.
    Users must implement the `handleJsonBatch` method to process Debezium events.
    """

    @abstractmethod
    def handleJsonBatch(self, records: list[ChangeEvent]):
        """
        Handles a batch of change events.

        This method receives a list of ChangeEvent objects (which are representations of
        Java ConnectRecords) and should process them.

        Args:
            records: A list of ChangeEvent objects representing the changes.

        Raises:
            NotImplementedError: If the method is not implemented.
        """
        raise NotImplementedError(
            "Not implemented, Please implement BasePythonChangeHandler and use it to consume events!"
        )


class DebeziumJsonEngine:
    """
    Main class to manage the Debezium embedded engine.
    """

    def __init__(
        self,
        properties: Union[dict[str, Any], "Properties"],
        handler: BasePythonChangeHandler,
    ):
        """
        Initializes the DebeziumJsonEngine.

        Args:
            properties: Debezium configuration (Python dictionary or Java Properties object).
            handler: The Python change event handler instance.
        """
        self.properties = properties

        if self.properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError(
                "Please provide handler class, see example class `pydbzengine.BasePythonChangeHandler`!"
            )

        self._handler = handler  # Store the handler.

    @cached_property
    def consumer(self):
        # Create the Python change consumer.
        from pydbzengine._jvm import PythonChangeConsumer

        return PythonChangeConsumer()

    @cached_property
    def engine(self):
        # Configure and build the Debezium engine.
        from pydbzengine._jvm import DebeziumEngine, EngineFormat, Properties

        # Convert Python dictionary to Java Properties if necessary
        java_props = Properties()
        if isinstance(self.properties, dict):
            for key, value in self.properties.items():
                java_props.setProperty(str(key), str(value))
        else:
            java_props = self.properties

        return (
            DebeziumEngine.create(EngineFormat.JSON)  # Use JSON format.
            .using(java_props)  # Set the configuration properties.
            .notifying(self.consumer)  # Set the change consumer.
            .build()
        )

    def run(self):
        """
        Starts the Debezium embedded engine.
        """
        # Set the handler for the consumer.
        self.consumer.set_change_handler(self._handler)
        self.consumer._exception = None  # Reset any previous exception
        self.engine.run()
        if self.consumer._exception:
            raise self.consumer._exception

    def close(self):
        """
        Closes the Debezium embedded engine.
        """
        if self.engine:
            try:
                self.engine.close()
            except Exception:
                pass

    def interrupt(self):
        """
        Interrupts the Debezium embedded engine.
        """
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
