import random
import time

from kafka import KafkaConsumer

TOPIC_NAME = "block3-processed"
GROUP_ID = "block3-processed-group"


def backoff(tries: int = 5, sleep: int = 5):
    def backoff_decorator(func):
        def _wrapper(*args, **kwargs):
            for i in range(tries):
                try:
                    func(*args, **kwargs)
                    break
                except Exception as e:
                    print(f"Attempt {i + 1} out of {tries}:")
                    print(e)
                    time.sleep(sleep)
            else:
                raise Exception("All attempts failed!")
        return _wrapper
    return backoff_decorator


def fake_db_connect() -> None:
    if random.random() > 0.8:
        raise Exception("Database connection error!")


@backoff()
def message_handler(message: str) -> None:
    fake_db_connect()
    print(message)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(TOPIC_NAME,
                             group_id=GROUP_ID,
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        message_handler(message)


if __name__ == '__main__':
    create_consumer()
