import signal
import threading

class Utils:

    @staticmethod
    def run_engine_async(engine, timeout_sec=22):
        try:
            thread = threading.Thread(target=engine.run)
            thread.start()
            thread.join(timeout=timeout_sec)  # This will block until the thread finishes or the signal is received.
        except TimeoutError:
            print("Engine run timed out!")