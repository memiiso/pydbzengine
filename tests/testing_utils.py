import signal
import threading


def timeout_handler(signum, frame):
    raise TimeoutError("Thread timed out!")


class TestingUtils:

    @staticmethod
    def run_engine_async(engine, timeout_sec=22):
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_sec)
        try:
            thread = threading.Thread(target=engine.run)
            thread.start()
            thread.join()
        except:
            pass
