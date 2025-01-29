import traceback
from abc import ABC
from pathlib import Path
from typing import List

################# INIT PYJNIUS ####################
DEBEZIUM_JAVA_LIBS_DIR = Path(__file__).parent.joinpath("debezium/libs/*").as_posix()
DEBEZIUM_CONF_DIR = Path(__file__).parent.joinpath("config").as_posix()
CLASS_PATHS = [DEBEZIUM_JAVA_LIBS_DIR, DEBEZIUM_CONF_DIR]
CONFIG_DIR = Path().cwd().joinpath('config')
if CONFIG_DIR.is_dir() and CONFIG_DIR.exists():
    print(f"Adding classpath: {CONFIG_DIR.as_posix()}")
    CLASS_PATHS.append(CONFIG_DIR.as_posix())

import jnius_config

# jnius_config.add_options('-Dlog4j.debug')
jnius_config.add_classpath(*CLASS_PATHS)
from jnius import autoclass
from jnius import PythonJavaClass, java_method, JavaMethod

################# JAVA REFLECTION CLASSES #################
Properties = autoclass('java.util.Properties')
DebeziumEngine = autoclass('io.debezium.engine.DebeziumEngine')
# following class is used by DebeziumEngine(above class)
DebeziumEngineBuilder = autoclass('io.debezium.engine.DebeziumEngine$Builder')
# NOTE: Override the notifying method somehow jnius was calling wrong java method. (first one)
DebeziumEngineBuilder.notifying = JavaMethod(
    '(Lio/debezium/engine/DebeziumEngine$ChangeConsumer;)Lio/debezium/engine/DebeziumEngine$Builder;')
StopEngineException = autoclass('io.debezium.engine.StopEngineException')
JavaLangSystem = autoclass('java.lang.System')
JavaLangThread = autoclass('java.lang.Thread')


class RecordCommitter(ABC):
    """
    USed for type hinting, for a coding convenience!
    imitates: io.debezium.engine.DebeziumEngine$RecordCommitter class
    """

    def markProcessed(self, record):
        pass

    def markBatchFinished(self):
        pass


class ChangeEvent(ABC):
    """
    USed for type hinting, for a coding convenience!
    imitates: org.apache.kafka.connect.connector.ConnectRecord class
    """

    def key(self) -> str:
        pass

    def value(self) -> str:
        pass

    # def headers(self) -> str:
    #     pass
    def destination(self) -> str:
        pass

    def partition(self) -> int:
        pass


class EngineFormat:
    JSON = autoclass('io.debezium.engine.format.Json')


class BasePythonChangeHandler(ABC):
    """
    Abstract class which end user needs to implement and pass it to PythonChangeConsumer!
    this is pure python class which receives variables which are type of java reflection classes.
    """

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        receives events from java! both records and committer arguments are java reflection classes.
        here we can read generated json events, and consume them

        :param records: list of records. reflection of: org.apache.kafka.connect.connector.ConnectRecord
        :return:
        """
        raise NotImplementedError(
            "Not implemented, Please implement BasePythonChangeHandler and use it to consume events!")


class PythonChangeConsumer(PythonJavaClass):
    """
    IMPORTANT: Java class implementation in Python. Proxy class
    implements ChangeConsumer interface of the debezium

    holds one additional pyhon argument self.handler, which is responsible to consume the events within python
    """
    __javainterfaces__ = ['io/debezium/engine/DebeziumEngine$ChangeConsumer']

    def __init__(self):
        self.handler: BasePythonChangeHandler

    @java_method('(Ljava/util/List;Lio/debezium/engine/DebeziumEngine$RecordCommitter;)V')
    def handleBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        """
        :param records: list of records. reflection of: org.apache.kafka.connect.connector.ConnectRecord
        :param committer: reflection of: io.debezium.engine.DebeziumEngine$RecordCommitter
        :return:
        """
        try:
            self.handler.handleJsonBatch(records=records)
            for e in records:
                committer.markProcessed(e)
            committer.markBatchFinished()
        except Exception as e:
            print("ERROR: failed to consume events in python")
            print(str(e))
            print(traceback.format_exc())
            JavaLangThread.currentThread().interrupt()

    @java_method('()Z')
    def supportsTombstoneEvents(self):
        return True

    def set_change_handler(self, handler: BasePythonChangeHandler):
        """
        # NOTE: Additional method, not part of the java interface definition.
        Sets handler class. all the java arguments are passed to this class for processing within python system.
        """
        self.handler = handler

    def interrupt(self):
        """
        # NOTE: Additional method, not part of the java interface definition.
        Used to stop debezium run. used to interrupt debezium process when any error happens processing data with python
        """
        print("Interrupt called in python consumer")
        JavaLangThread.currentThread().interrupt()


class DebeziumJsonEngine:
    """
    Main pyton class which puts everything together. the Main application.
    """
    def __init__(self, properties: Properties, handler: BasePythonChangeHandler):
        self.properties: Properties = properties

        if self.properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError("Please provide handler class, see example class `pydbzengine.BasePythonChangeHandler`!")

        # Python class which implements java interface, proxy class
        self.consumer = PythonChangeConsumer()
        # give it additional pyhon class to hand over the data/arguments received from java
        self._handler = handler
        self.consumer.set_change_handler(self._handler)
        # instantiation of the java application, Embedded engine.
        self.engine: DebeziumEngine = (DebeziumEngine.create(EngineFormat.JSON)
                                       .using(self.properties)
                                       .notifying(self.consumer)
                                       .build())

    def run(self):
        """
        Start the execution of debezium Embedded engine
        """
        self.engine.run()

    def interrupt(self):
        """
        calls java interrupt to stop the Embedded engine
        """
        self.consumer.interrupt()
