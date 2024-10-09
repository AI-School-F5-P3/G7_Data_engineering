from kafka import KafkaConsumer as KC
import json
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

class KafkaConsumer:
    def __init__(self):
        self.consumer = KC(KAFKA_TOPIC, 
                           bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                           auto_offset_reset='earliest', 
                           enable_auto_commit=True,
                           group_id='hr_group', 
                           value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def consume(self):
        for message in self.consumer:
            yield message.value