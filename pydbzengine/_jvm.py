import sys
import traceback
from pathlib import Path
from typing import List
import jpype

################# STEP 1  INIT GLOBAL VARIABLES ####################
# Define paths to Debezium Java libraries and configuration directory.
DEBEZIUM_JAVA_LIBS_DIR = Path(__file__).parent.joinpath("debezium/libs")
DEBEZIUM_CONF_DIR = Path(__file__).parent.joinpath("config").as_posix()

# Manually expand jars (JPype doesn't support glob patterns in classpath like pyjnius)
_jars = list(DEBEZIUM_JAVA_LIBS_DIR.glob("*.jar"))
CLASS_PATHS = [str(j) for j in _jars]
CLASS_PATHS.append(DEBEZIUM_CONF_DIR)

# Add current working directory's config folder to classpath if exists
CONFIG_DIR = Path().cwd().joinpath("config")
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    print(f"Adding classpath: {CONFIG_DIR.as_posix()}")
    CLASS_PATHS.append(CONFIG_DIR.as_posix())

################# STEP 2  INIT JPYPE, JVM ####################
# Start JVM with the classpaths (equivalent to jnius_config.add_classpath)
if not jpype.isJVMStarted():
    jvm_path = jpype.getDefaultJVMPath()

    # jpype.getDefaultJVMPath() returns bytes in some versions/OS
    if isinstance(jvm_path, bytes):
        jvm_path = jvm_path.decode("utf-8")

    # Fix for macOS where getDefaultJVMPath might return the Home directory instead of the lib
    if sys.platform == "darwin" and Path(jvm_path).is_dir():
        # Try common locations specifically for macOS/Corretto
        potential_paths = [
            Path(jvm_path) / "lib" / "server" / "libjvm.dylib",
            Path(jvm_path) / "lib" / "libjvm.dylib",
            ]
        for p in potential_paths:
            if p.exists():
                jvm_path = str(p)
                break

    jpype.startJVM(jvm_path, classpath=CLASS_PATHS)
    print("JVM started.")

################# STEP 3 JAVA REFLECTION CLASSES #################
# Import Java classes using jpype's JClass for reflection.
Properties = jpype.JClass("java.util.Properties")
DebeziumEngine = jpype.JClass("io.debezium.engine.DebeziumEngine")
DebeziumEngineBuilder = jpype.JClass("io.debezium.engine.DebeziumEngine$Builder")

# Note: JPype handles method overloading correctly, no JavaMethod workaround needed

StopEngineException = jpype.JClass("io.debezium.engine.StopEngineException")
JavaLangSystem = jpype.JClass("java.lang.System")
JavaLangThread = jpype.JClass("java.lang.Thread")

################# STEP 4 CREATE JAVA CLASSES #################
class EngineFormat:
    """
    Class holding constants for Debezium engine formats.
    """

    JSON = jpype.JClass("io.debezium.engine.format.Json")


@jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")
class PythonChangeConsumer:
    """
    Python implementation of the Debezium ChangeConsumer interface.
    This class acts as a bridge between Java Debezium Engine and the Python handler.
    """

    def __init__(self):
        self.handler: "BasePythonChangeHandler" = None  # The Python handler instance.

    @jpype.JOverride
    def handleBatch(self, records: List["ChangeEvent"], committer: "RecordCommitter"):
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

    @jpype.JOverride
    def supportsTombstoneEvents(self):
        """
        Indicates whether the consumer supports tombstone events.
        """
        return True

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
        print("Interrupt called in python consumer")
        JavaLangThread.currentThread().interrupt()  # Interrupt the current thread (Debezium engine thread).

    def __exit__(self, exc_type, exc_value, traceback):
        print("Python Exit method called! calling interrupt to stop the engine")
        self.interrupt()
