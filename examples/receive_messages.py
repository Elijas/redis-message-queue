import signal
import time

from redis import Redis

from redis_message_queue.redis_message_queue import RedisMessageQueue

REDIS_CONNECTION_STRING = "redis://localhost:6379/0"


class GracefulInterruptHandler:
    def __init__(self, verbose: bool = True):
        self.interrupted = False
        self._verbose = verbose
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)

    def signal_handler(self, signum, frame):
        if self._verbose:
            print(f"Received signal: {signal.strsignal(signum)}")
        self.interrupted = True


def main():
    # The GracefulInterruptHandler allows us to handle Ctrl+C (SIGINT) gracefully.
    # This means that when the user sends an interrupt signal, the program will
    # not terminate immediately but will instead set a flag that can be used to
    # stop the program in an orderly fashion, allowing for any necessary cleanup.
    handler = GracefulInterruptHandler()

    client = Redis.from_url(REDIS_CONNECTION_STRING, decode_responses=True)
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
    )
    while True:
        with queue.process_message() as message:
            if not message:
                if handler.interrupted:
                    print("Exiting...")
                    break
                # Note: you can specify a custom heartbeat interval
                print("Waiting for message...")

            delay = 0.5
            print(
                f"Received Message: '{message}'."
                f" Will pretend to process it {delay} seconds..."
            )
            time.sleep(delay)
            print(f"Finished processing message '{message}' Waiting for next message.")


if __name__ == "__main__":
    main()
