import threading
import logging

class Utils:

    @staticmethod
    def run_engine_async(engine, timeout_sec=22, blocking=True):
        """
        Runs an engine asynchronously with a timeout.

        This method runs the given engine's `run` method in a separate thread
        and applies a timeout.  If the engine's `run` method doesn't complete
        within the specified timeout, the engine is closed.

        Args:
            engine: The engine object to run.  It is expected to have run() and close() methods.
            timeout_sec: The timeout duration in seconds.  Defaults to 22 seconds.
            blocking: If True, blocks until the engine finishes or times out.
        """
        logger = logging.getLogger("pydbzengine.helper.Utils")

        thread = threading.Thread(target=engine.run, daemon=True)
        thread.start()

        if timeout_sec is not None and timeout_sec > 0:
            def _timeout_shutdown():
                if thread.is_alive():
                    logger.info("Engine run timed out! Closing engine.")
                    engine.close()

            timer = threading.Timer(timeout_sec, _timeout_shutdown)
            timer.daemon = True
            timer.start()

            if blocking:
                thread.join()
                timer.cancel()
        elif blocking:
            thread.join()

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
        from pathlib import Path

        logger = logging.getLogger(__name__)

        def _run():
            engine.run()

        # Check the current size of the log file to avoid scanning historical logs
        log_file = Path("logs/pydbzengine.log")
        initial_seek = 0
        if log_file.exists():
            initial_seek = log_file.stat().st_size

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        try:
            while thread.is_alive():
                # 1. Primary check: check Log4j2 log file
                if log_file.exists():
                    try:
                        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                            f.seek(initial_seek)
                            content = f.read()
                            if "Snapshot completed" in content:
                                logger.info("Snapshot completion detected in log file. Closing engine.")
                                engine.close()
                                return
                    except Exception:
                        pass

                # 2. Fallback check: check Python logging root handlers (safe check)
                log_output = logging.root.handlers
                for handler in log_output:
                    if hasattr(handler, "stream"):
                        try:
                            if hasattr(handler.stream, "getvalue"):
                                stream_value = handler.stream.getvalue()
                                if "Snapshot completed" in stream_value:
                                    logger.info("Snapshot completion detected in stream handler. Closing engine.")
                                    engine.close()
                                    return
                        except Exception:
                            continue

                time.sleep(poll_interval_sec)

        finally:
            thread.join()
