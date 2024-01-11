# redis-message-queue

Robust Python queuing with message deduplication.

# Features

* **Exactly-once delivery and publish guarantees:** Our system ensures that messages are both delivered and published no more than once. This is achieved through Redis' atomic transactions, message deduplication, and idempotent processing, which together prevent race conditions, duplicate processing, and multiple publications.
* **Message deduplication and idempotent processing:** By default, messages are deduplicated to prevent multiple sends of the same message. This ensures that each message is processed only once, maintaining idempotency even with producer retries.
* **Automatic message acknowledgement and resilient processing:** Messages are automatically acknowledged post-processing, with a robust mechanism in place to handle consumer crashes. Failed messages are moved to a dedicated log within Redis, preventing loss and allowing for recovery and reprocessing.
* **Efficient and visible message handling:** Success and failure logs provide insight into message processing outcomes. Additionally, Redis' blocking queue commands optimize resource usage by eliminating the need for constant polling, thus conserving CPU resources.
* **Graceful shutdown for idle consumers:** The system includes a mechanism to handle graceful shutdowns, allowing consumers to complete processing of the current message before shutting down. This is particularly useful for handling interrupt signals (e.g., Ctrl+C) without disrupting ongoing tasks.

Please note that these features are optional and can be disabled as needed.

# Preparation

```bash
pip install redis-message-queue
```

You will also need a running Redis server. You can run one locally with Docker:

```bash
docker run -it --rm -p 6379:6379 redis
```

# Usage

Send messages to a queue:

```python
import time
from random import randint as random_number

from redis import Redis

from redis_message_queue import RedisMessageQueue

client = Redis.from_url("redis://localhost:6379/0")
queue = RedisMessageQueue(
    name="my_message_queue",
    client=client,
    deduplication=True,
)

while True:
    # Sending unique messages
    queue.publish(f"Hello (id={random_number(0, 1_000_000)})")
    time.sleep(1)
```

Receive messages from a queue:

```python
from redis import Redis

from redis_message_queue import RedisMessageQueue

client = Redis.from_url(
    "redis://localhost:6379/0",
    decode_responses=True,
)
queue = RedisMessageQueue("my_message_queue", client=client)

while True:
    with queue.process_message() as message:
        if message:
            print(f"Received Message: {message}")
```

To see how the message queue operates, you can look at the examples in the [examples](https://github.com/Elijas/redis-message-queue/tree/main/examples) folder. 

Run two publishers and three workers by using the commands below. Each command should be run in its own terminal window:

```bash
python -m examples.send_messages
python -m examples.send_messages
python -m examples.receive_messages
python -m examples.receive_messages
python -m examples.receive_messages
```
