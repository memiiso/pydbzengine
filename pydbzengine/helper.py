import signal
import threading

def timeout_handler(signum, frame):
    """
    Signal handler for timeouts.

    Raises a TimeoutError when the specified timeout is reached.

    Args:
        signum: The signal number (unused).
        frame: The current stack frame (unused).

    Raises:
        TimeoutError: If the timeout is reached.
    """
    raise TimeoutError("Engine run timed out!")


class Utils:

    @staticmethod
    def run_engine_async(engine, timeout_sec=22, blocking=True):
        """
        Runs an engine asynchronously with a timeout.

        This method runs the given engine's `run` method in a separate thread
        and applies a timeout.  If the engine's `run` method doesn't complete
        within the specified timeout, a TimeoutError is raised, and the thread
        is interrupted.

        Args:
            engine: The engine object to run.  It is expected to have a `run` method.
            timeout_sec: The timeout duration in seconds.  Defaults to 22 seconds.
        """
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_sec)

        try:
            thread = threading.Thread(target=engine.run, daemon=True)
            thread.start()

            if blocking:
                # Wait for the thread to complete (or the timeout to occur).
                thread.join()  # This will block until the thread finishes or the signal is received.

        except TimeoutError:
            # Handle the timeout exception.
            print("Engine run timed out!")  # use logger here for better logging
            engine.close()
            return  # Or potentially handle the timeout differently (e.g., attempt to stop the engine).

        finally:
            # **Crucially important:** Cancel the alarm.  This prevents the timeout
            # from triggering again later if the main thread continues to run.
            signal.alarm(0)  # 0 means cancel the alarm.

    @staticmethod
    def run_engine_until_snapshot(engine, poll_interval_sec=1):
        """
        Run Debezium engine and automatically stop when the initial
        snapshot is completed (snapshot.mode=initial_only).

        The engine runs in a background thread while this function
        periodically checks for the snapshot completion log message.
        """

        import threading
        import time
        import logging

        logger = logging.getLogger(__name__)

        def _run():
            engine.run()

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        try:
            while thread.is_alive():

                # read logs emitted by debezium
                log_output = logging.root.handlers

                for handler in log_output:
                    if hasattr(handler, "stream"):
                        try:
                            stream_value = handler.stream.getvalue()
                        except Exception:
                            continue

                        if "Snapshot completed" in stream_value:
                            logger.info("Snapshot completion detected. Closing engine.")
                            engine.close()
                            return

                time.sleep(poll_interval_sec)

        finally:
            thread.join()
