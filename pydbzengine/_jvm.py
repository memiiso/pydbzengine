import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydbzengine import BasePythonChangeHandler, ChangeEvent, RecordCommitter

import jpype

logger = logging.getLogger("pydbzengine._jvm")

################# STEP 1  INIT GLOBAL VARIABLES ####################
# Define paths to Debezium Java libraries and configuration directory.
DEBEZIUM_JAVA_LIBS_DIR = Path(__file__).parent.joinpath("debezium/libs")
DEBEZIUM_CONF_DIR = Path(__file__).parent.joinpath("config").as_posix()

# Manually expand jars (JPype doesn't support glob patterns in classpath like pyjnius)
_jars = list(DEBEZIUM_JAVA_LIBS_DIR.glob("*.jar"))
if not _jars:
    raise ImportError(
        "Debezium jar files not found in pydbzengine/debezium/libs/. "
        "Please download the libraries by running the install_libs.sh script first!"
    )

CLASS_PATHS = [str(j) for j in _jars]
CLASS_PATHS.append(DEBEZIUM_CONF_DIR)

# Add current working directory's config folder to classpath if exists
CONFIG_DIR = Path().cwd().joinpath("config")
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    logger.info(f"Adding classpath: {CONFIG_DIR.as_posix()}")
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

    # Load custom JVM options from environment variable or default to standard streaming limits
    jvm_opts = []
    env_opts = os.environ.get("PYDBZ_JVM_OPTS")
    if env_opts:
        jvm_opts = env_opts.split()
    else:
        jvm_opts = ["-Xms256m", "-Xmx2g", "-XX:+UseG1GC"]

    jpype.startJVM(jvm_path, *jvm_opts, classpath=CLASS_PATHS)
    logger.info("JVM started.")

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


@jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")
class PythonChangeConsumer:
    """
    Python implementation of the Debezium ChangeConsumer interface.
    This class acts as a bridge between Java Debezium Engine and the Python handler.
    """

    def __init__(self):
        self.handler: BasePythonChangeHandler = None  # The Python handler instance.
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
