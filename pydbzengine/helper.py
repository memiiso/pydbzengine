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
    def run_engine_async(engine, timeout_sec=22):
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
            thread = threading.Thread(target=engine.run)
            thread.start()

            # Wait for the thread to complete (or the timeout to occur).
            thread.join()  # This will block until the thread finishes or the signal is received.

        except TimeoutError:
            # Handle the timeout exception.
            print("Engine run timed out!") # use logger here for better logging
            return  # Or potentially handle the timeout differently (e.g., attempt to stop the engine).

        finally:
            # **Crucially important:** Cancel the alarm.  This prevents the timeout
            # from triggering again later if the main thread continues to run.
            signal.alarm(0)  # 0 means cancel the alarm.

        # If the engine.run() finishes within the timeout, this point will be reached.
        # No explicit return is needed as the function doesn't return anything.
        print("Engine run completed successfully.") # Add a log message to signal success