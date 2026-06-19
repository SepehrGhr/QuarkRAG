import json
from aiokafka import AIOKafkaProducer
from services.embedding.config import settings
from services.embedding.logging_config import logger

class KafkaProducerManager:
    def __init__(self):
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await self.producer.start()
        logger.info("Embedding service Kafka producer started")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Embedding service Kafka producer stopped")

    async def send_message(self, topic: str, key: str, value: dict):
        if not self.producer:
            raise RuntimeError("Producer has not been started")
        await self.producer.send_and_wait(
            topic=topic,
            key=key.encode("utf-8"),
            value=value
        )
        logger.debug("Message sent from embedding service", topic=topic, key=key)

kafka_producer = KafkaProducerManager()
