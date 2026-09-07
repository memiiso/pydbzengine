import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydbzengine import BasePythonChangeHandler, ChangeEvent, RecordCommitter

import jpype

logger = logging.getLogger("pydbzengine._jvm")


################# STEP 3 JAVA REFLECTION CLASSES #################
# Import Java classes using jpype's JClass for reflection.
try:
    Properties = jpype.JClass("java.util.Properties")
    DebeziumEngine = jpype.JClass("io.debezium.engine.DebeziumEngine")
    DebeziumEngineBuilder = jpype.JClass("io.debezium.engine.DebeziumEngine$Builder")
    StopEngineException = jpype.JClass("io.debezium.engine.StopEngineException")
    JavaLangSystem = jpype.JClass("java.lang.System")
    JavaLangThread = jpype.JClass("java.lang.Thread")
except Exception as e:
    if jpype.isJVMStarted():
        raise RuntimeError(
            "JVM is already started, but Debezium engine classes could not be loaded. "
            "Please ensure that the JVM is initialized with the correct classpaths containing Debezium jar files."
        ) from e
    raise


################# STEP 4 CREATE JAVA CLASSES #################
class EngineFormat:
    """
    Class holding constants for Debezium engine formats.
    """

    JSON = jpype.JClass("io.debezium.engine.format.Json")
    CONNECT = jpype.JClass("io.debezium.embedded.Connect")


@jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")
class PythonChangeConsumer:
    """
    Python implementation of the Debezium ChangeConsumer interface.
    This class acts as a bridge between Java Debezium Engine and the Python handler.
    """

    def __init__(self):
        self.handler: BasePythonChangeHandler | None = (
            None  # The Python handler instance.
        )
        self._exception = (
            None  # Store any Python exception raised during callback execution.
        )

    @jpype.JOverride
    def handleBatch(self, records: list["ChangeEvent"], committer: "RecordCommitter"):
        """
        Handles a batch of change events received from the Debezium engine.

        This method is called by the Java Debezium engine. It calls the user-defined
        Python handler to process the events and then acknowledges the batch.

        Args:
            records: A list of ChangeEvent objects representing the changes.
            committer: The RecordCommitter used to acknowledge processed records.
        """
        try:
            if self.handler is None:
                raise RuntimeError("PythonChangeConsumer handler is not set!")
            self.handler.handleJsonBatch(records=records)
            for e in records:
                committer.markProcessed(e)  # Mark each record as processed.
            committer.markBatchFinished()  # Mark the batch as finished.
        except Exception as e:
            logger.error("Failed to consume events in python", exc_info=True)
            self._exception = (
                e  # Capture the exception to re-raise it on caller thread.
            )
            JavaLangThread.currentThread().interrupt()  # Interrupt the Debezium engine on error.

    @jpype.JOverride
    def supportsTombstoneEvents(self):
        """
        Indicates whether the consumer supports tombstone events.
        """
        return True

    @property
    def error(self) -> Exception | None:
        """Returns the last captured exception during event consumption, if any."""
        return self._exception

    def clear_error(self) -> None:
        """Clears any previously captured exception."""
        self._exception = None

    def raise_if_failed(self) -> None:
        """Raises any captured exception from event consumption."""
        if self._exception is not None:
            raise self._exception

    def set_change_handler(self, handler: "BasePythonChangeHandler"):
        """
        Sets the Python change event handler.

        Args:
            handler: The Python change event handler instance.
        """
        self.handler = handler

    def interrupt(self):
        """
        Interrupts the Debezium engine.
        """
        logger.info("Interrupt called in python consumer")
        JavaLangThread.currentThread().interrupt()  # Interrupt the current thread (Debezium engine thread).

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("Python Exit method called! calling interrupt to stop the engine")
        self.interrupt()
