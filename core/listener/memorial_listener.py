
import json
import logging
from typing import Optional, Callable, Awaitable
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from core.serializer import AvroDeserializer, SchemaRegistryClient


logger = logging.getLogger(__name__)


class MemorialListener:
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        schema_registry_url: str,
        group_id: str = "feed",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        schema_registry_auth: Optional[tuple[str, str]] = None,
        **kwargs
    ):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.schema_registry_auth = schema_registry_auth
        self.additional_config = kwargs
        self.topic = "memorial-vectorizing-request"

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._schema_registry: Optional[SchemaRegistryClient] = None
        self._deserializer: Optional[AvroDeserializer] = None
        self._started = False
        self._message_handler: Optional[Callable[[dict], Awaitable[None]]] = None

    async def start(self) -> None:
        if self._started:
            logger.warning("Consumer already started")
            return

        try:
            self._schema_registry = SchemaRegistryClient(
                url=self.schema_registry_url,
                auth=self.schema_registry_auth
            )

            self._deserializer = AvroDeserializer(
                schema_registry_client=self._schema_registry
            )

            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=self.enable_auto_commit,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                **self.additional_config
            )

            await self._consumer.start()
            self._started = True

            logger.info(
                f"Memorial Kafka listener started: topic='{self.topic}', "
                f"group_id='{self.group_id}', brokers={self.bootstrap_servers}"
            )

        except Exception as e:
            logger.error(f"Failed to start Memorial Kafka listener: {e}")
            raise

    def set_message_handler(
        self,
        handler: Callable[[dict], Awaitable[None]]
    ) -> None:
        self._message_handler = handler
        logger.info("Message handler registered")

    async def consume(self) -> None:
        if not self._started:
            await self.start()

        if self._message_handler is None:
            logger.warning("No message handler set. Messages will be logged only.")

        logger.info(f"Starting to consume messages from topic '{self.topic}'")

        try:
            async for message in self._consumer:
                try:
                    deserialized_data = await self._deserializer.deserialize(
                        message.value,
                        message.key
                    )

                    logger.info(
                        f"Received message - Topic: {message.topic}, "
                        f"Partition: {message.partition}, Offset: {message.offset}, "
                        f"Key: {message.key}"
                    )
                    logger.debug(f"Deserialized data: {deserialized_data}")

                    if self._message_handler:
                        await self._message_handler(deserialized_data)
                    else:
                        logger.info(f"Memorial data: {json.dumps(deserialized_data, indent=2)}")

                except Exception as e:
                    logger.error(
                        f"Error processing message from partition {message.partition}, "
                        f"offset {message.offset}: {e}",
                        exc_info=True
                    )
                    continue

        except KafkaError as e:
            logger.error(f"Kafka error while consuming: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while consuming: {e}")
            raise

    async def close(self) -> None:
        if self._consumer and self._started:
            try:
                await self._consumer.stop()
                logger.info("Kafka consumer stopped")
            except Exception as e:
                logger.error(f"Error stopping Kafka consumer: {e}")

        if self._schema_registry:
            try:
                await self._schema_registry.close()
                logger.info("Schema Registry client closed")
            except Exception as e:
                logger.error(f"Error closing Schema Registry client: {e}")

        self._started = False
        logger.info("Memorial Kafka listener closed")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
