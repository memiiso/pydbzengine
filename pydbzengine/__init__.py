import traceback
from abc import ABC
from pathlib import Path
from typing import List

################# INIT PYJNIUS ####################
# Define paths to Debezium Java libraries and configuration directory.
DEBEZIUM_JAVA_LIBS_DIR = Path(__file__).parent.joinpath("debezium/libs/*").as_posix()
DEBEZIUM_CONF_DIR = Path(__file__).parent.joinpath("config").as_posix()
CLASS_PATHS = [DEBEZIUM_JAVA_LIBS_DIR, DEBEZIUM_CONF_DIR]

# Add current working directory's config folder to classpath if exists
CONFIG_DIR = Path().cwd().joinpath('config')
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    print(f"Adding classpath: {CONFIG_DIR.as_posix()}")
    CLASS_PATHS.append(CONFIG_DIR.as_posix())

import jnius_config

# Add the necessary classpaths for JNI interaction with Java.
jnius_config.add_classpath(*CLASS_PATHS)

from jnius import autoclass
from jnius import PythonJavaClass, java_method, JavaMethod

################# JAVA REFLECTION CLASSES #################
# Import Java classes using jnius's autoclass for reflection.
Properties = autoclass('java.util.Properties')
DebeziumEngine = autoclass('io.debezium.engine.DebeziumEngine')
DebeziumEngineBuilder = autoclass('io.debezium.engine.DebeziumEngine$Builder')

# Override the notifying method of DebeziumEngineBuilder to use the correct JavaMethod signature.
# This is a workaround for a potential jnius issue where the wrong Java method is called.
DebeziumEngineBuilder.notifying = JavaMethod(
    '(Lio/debezium/engine/DebeziumEngine$ChangeConsumer;)Lio/debezium/engine/DebeziumEngine$Builder;')

StopEngineException = autoclass('io.debezium.engine.StopEngineException')
JavaLangSystem = autoclass('java.lang.System')
JavaLangThread = autoclass('java.lang.Thread')


class RecordCommitter(ABC):
    """
    Abstract base class for type hinting the RecordCommitter.
    Mimics the io.debezium.engine.DebeziumEngine$RecordCommitter interface for Python.
    """

    def markProcessed(self, record):
        """Marks a single record as processed."""
        pass

    def markBatchFinished(self):
        """Marks the entire batch as finished."""
        pass


class ChangeEvent(ABC):
    """
    Abstract base class for type hinting the ChangeEvent.
    Mimics the org.apache.kafka.connect.connector.ConnectRecord interface for Python.
    """

    def key(self) -> str:
        """Returns the record key."""
        pass

    def value(self) -> str:
        """Returns the record value (payload)."""
        pass

    def destination(self) -> str:
        """Returns the destination topic/table."""
        pass

    def partition(self) -> int:
        """Returns the partition the record belongs to."""
        pass


class EngineFormat:
    """
    Class holding constants for Debezium engine formats.
    """
    JSON = autoclass('io.debezium.engine.format.Json')


class BasePythonChangeHandler(ABC):
    """
    Abstract base class for user-defined change event handlers.
    Users must implement the `handleJsonBatch` method to process Debezium events.
    """

    def handleJsonBatch(self, records: List[ChangeEvent]):
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
            "Not implemented, Please implement BasePythonChangeHandler and use it to consume events!")


class PythonChangeConsumer(PythonJavaClass):
    """
    Python implementation of the Debezium ChangeConsumer interface.
    This class acts as a bridge between Java Debezium Engine and the Python handler.
    """
    __javainterfaces__ = ['io/debezium/engine/DebeziumEngine$ChangeConsumer']

    def __init__(self):
        self.handler: BasePythonChangeHandler = None  # The Python handler instance.

    @java_method('(Ljava/util/List;Lio/debezium/engine/DebeziumEngine$RecordCommitter;)V')
    def handleBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        """
        Handles a batch of change events received from the Debezium engine.

        This method is called by the Java Debezium engine. It calls the user-defined
        Python handler to process the events and then acknowledges the batch.

        Args:
            records: A list of ChangeEvent objects representing the changes.
            committer: The RecordCommitter used to acknowledge processed records.
        """
        try:
            self.handler.handleJsonBatch(records=records)
            for e in records:
                committer.markProcessed(e)  # Mark each record as processed.
            committer.markBatchFinished()  # Mark the batch as finished.
        except Exception as e:
            print("ERROR: failed to consume events in python")
            print(str(e))
            print(traceback.format_exc())
            JavaLangThread.currentThread().interrupt()  # Interrupt the Debezium engine on error.

    @java_method('()Z')
    def supportsTombstoneEvents(self):
        """
        Indicates whether the consumer supports tombstone events.
        """
        return True

    def set_change_handler(self, handler: BasePythonChangeHandler):
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
        print("Interrupt called in python consumer")
        JavaLangThread.currentThread().interrupt()  # Interrupt the current thread (Debezium engine thread).

    def __exit__(self, exc_type, exc_value, traceback):
        print("Python Exit method called! calling interrupt to stop the engine")
        self.interrupt()

class DebeziumJsonEngine:
    """
    Main class to manage the Debezium embedded engine.
    """

    def __init__(self, properties: Properties, handler: BasePythonChangeHandler):
        """
        Initializes the DebeziumJsonEngine.

        Args:
            properties: Java Properties object containing the Debezium configuration.
            handler: The Python change event handler instance.
        """
        self.properties: Properties = properties

        if self.properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError("Please provide handler class, see example class `pydbzengine.BasePythonChangeHandler`!")

        self.consumer = PythonChangeConsumer()  # Create the Python change consumer.
        self._handler = handler  # Store the handler.
        self.consumer.set_change_handler(self._handler)  # Set the handler for the consumer.

        # Create and configure the Debezium engine.
        self.engine: DebeziumEngine = (DebeziumEngine.create(EngineFormat.JSON)  # Use JSON format.
                                       .using(self.properties)  # Set the configuration properties.
                                       .notifying(self.consumer)  # Set the change consumer.
                                       .build())  # Build the engine.

    def run(self):
        """
        Starts the Debezium embedded engine.
        """
        self.engine.run()

    def interrupt(self):
        """
        Interrupts the Debezium embedded engine.
        """
        self.consumer.interrupt()