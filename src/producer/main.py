import time
import json
import uuid
import logging
from kafka import KafkaProducer
from extract import connect_to_api, extract_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_kafka_producer():
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception as e:
            logger.error(f"Kafka not ready, retrying... ({e})")
            retries -= 1
            time.sleep(5)
    raise Exception("Could not connect to Kafka")


def main():
    response = connect_to_api()
    data = extract_json(response)

    producer = get_kafka_producer()
    for stock in data:
        stock['id'] = str(uuid.uuid4())  # unique ID per record
        producer.send('stock_topic', stock)
    producer.flush()
    logger.info(f"Successfully sent {len(data)} records to Kafka!")


if __name__ == '__main__':
    main()