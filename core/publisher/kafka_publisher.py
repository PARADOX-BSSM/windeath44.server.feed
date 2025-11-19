import json
import logging
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from .publisher_interface import Publisher


logger = logging.getLogger(__name__)


class KafkaPublisher:
    
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        client_id: Optional[str] = None,
        compression_type: Optional[str] = "gzip",
        max_batch_size: int = 16384,
        linger_ms: int = 10,
        acks: str | int = "all",
        **kwargs
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id or "kafka-publisher"
        self.compression_type = compression_type
        self.linger_ms = linger_ms
        self.acks = acks
        self.additional_config = kwargs
        
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False
    
    async def _ensure_started(self) -> None:
        """Ensure the producer is started."""
        if not self._started:
            await self.start()
    
    async def start(self) -> None:
        """Start the Kafka producer."""
        if self._started:
            logger.warning("Producer already started")
            return
        
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                compression_type=self.compression_type,
                linger_ms=self.linger_ms,
                acks=self.acks,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                **self.additional_config
            )
            await self._producer.start()
            self._started = True
            logger.info(f"Kafka producer started: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise
    
    async def publish(
        self, 
        topic: str, 
        message: dict[str, Any],
        key: Optional[str] = None
    ) -> bool:
        await self._ensure_started()
        
        try:
            await self._producer.send_and_wait(
                topic=topic,
                value=message,
                key=key
            )
            logger.debug(f"Published message to topic '{topic}': {message}")
            return True
        except KafkaError as e:
            logger.error(f"Kafka error publishing to '{topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing to '{topic}': {e}")
            return False
    
    async def close(self) -> None:
        if self._producer and self._started:
            try:
                await self._producer.stop()
                self._started = False
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
