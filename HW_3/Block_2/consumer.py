from kafka import KafkaConsumer

TOPIC_NAME = "block2-processed"
GROUP_ID = "block2-processed-group"


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer(TOPIC_NAME,
                             group_id=GROUP_ID,
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        print(message)


if __name__ == '__main__':
    create_consumer()
