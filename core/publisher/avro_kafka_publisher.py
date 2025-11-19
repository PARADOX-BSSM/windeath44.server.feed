"""
Avro-enabled Kafka Publisher using Adapter Pattern.

This module provides a Kafka publisher that serializes messages
to Avro format with Schema Registry integration.
"""

import logging
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from .publisher_interface import Publisher
from ..serializer import AvroSerializer, SchemaRegistryClient


logger = logging.getLogger(__name__)


class AvroKafkaPublisher:
    """
    Kafka Publisher with Avro serialization support (Adapter Pattern).
    
    Extends the basic Kafka publisher to support Avro serialization
    with automatic schema registration via Schema Registry.
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str | list[str]] = None,
        schema_registry_url: Optional[str] = None,
        default_subject: Optional[str] = None,
        client_id: Optional[str] = None,
        compression_type: Optional[str] = "gzip",
        max_batch_size: int = 16384,
        linger_ms: int = 10,
        acks: str | int = "all",
        schema_registry_auth: Optional[tuple[str, str]] = None,
        **kwargs
    ):
        """
        Initialize Avro Kafka Publisher.
        
        Args:
            bootstrap_servers: Kafka broker addresses (defaults to env KAFKA_BOOTSTRAP_SERVERS)
            schema_registry_url: Schema Registry URL (defaults to env SCHEMA_REGISTRY_URL)
            default_subject: Default subject for schema registration
            client_id: Client identifier
            compression_type: Compression type
            max_batch_size: Maximum batch size in bytes
            linger_ms: Time to wait before sending batch
            acks: Acknowledgment level
            schema_registry_auth: Optional (username, password) for Schema Registry
            **kwargs: Additional producer configuration
        """
        import os
        
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.schema_registry_url = schema_registry_url or os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        self.default_subject = default_subject
        self.client_id = client_id or "avro-kafka-publisher"
        self.compression_type = compression_type
        self.max_batch_size = max_batch_size
        self.linger_ms = linger_ms
        self.acks = acks
        self.schema_registry_auth = schema_registry_auth
        self.additional_config = kwargs
        
        self._producer: Optional[AIOKafkaProducer] = None
        self._schema_registry: Optional[SchemaRegistryClient] = None
        self._serializers: dict[str, AvroSerializer] = {}
        self._started = False
    
    async def _ensure_started(self) -> None:
        """Ensure the producer and schema registry are started."""
        if not self._started:
            await self.start()
    
    async def start(self) -> None:
        """Start the Kafka producer and Schema Registry client."""
        if self._started:
            logger.warning("Producer already started")
            return
        
        try:
            # Initialize Schema Registry client
            self._schema_registry = SchemaRegistryClient(
                url=self.schema_registry_url,
                auth=self.schema_registry_auth
            )
            
            # Initialize Kafka producer (without value_serializer for raw bytes)
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                compression_type=self.compression_type,
                max_batch_size=self.max_batch_size,
                linger_ms=self.linger_ms,
                acks=self.acks,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                **self.additional_config
            )
            await self._producer.start()
            self._started = True
            logger.info(
                f"Avro Kafka producer started: {self.bootstrap_servers}, "
                f"Schema Registry: {self.schema_registry_url}"
            )
        except Exception as e:
            logger.error(f"Failed to start Avro Kafka producer: {e}")
            raise
    
    def _get_subject(self, topic: str, subject: Optional[str] = None) -> str:
        """
        Get subject name for schema registration.
        
        Args:
            topic: Kafka topic name
            subject: Explicit subject name (optional)
            
        Returns:
            Subject name to use
        """
        if subject:
            return subject
        if self.default_subject:
            return self.default_subject
        # Convention: {topic}-value
        return f"{topic}-value"
    
    async def register_schema(
        self,
        topic: str,
        schema: str | dict,
        subject: Optional[str] = None
    ) -> int:
        """
        Register an Avro schema for a topic.
        
        Args:
            topic: Kafka topic
            schema: Avro schema (JSON string or dict)
            subject: Optional explicit subject name
            
        Returns:
            Schema ID from registry
        """
        await self._ensure_started()
        
        subject_name = self._get_subject(topic, subject)
        schema_id = await self._schema_registry.register_schema(subject_name, schema)
        
        # Create and cache serializer
        self._serializers[subject_name] = AvroSerializer(
            schema_registry_client=self._schema_registry,
            subject=subject_name,
            schema=schema,
            auto_register=False  # Already registered
        )
        
        logger.info(f"Registered schema for topic '{topic}', subject '{subject_name}': ID {schema_id}")
        return schema_id
    
    async def publish(
        self,
        topic: str,
        message: dict[str, Any],
        key: Optional[str] = None,
        schema: Optional[str | dict] = None,
        subject: Optional[str] = None
    ) -> bool:
        """
        Publish a message to Kafka with Avro serialization.
        
        Args:
            topic: Kafka topic to publish to
            message: Message payload as dictionary
            key: Optional message key for partitioning
            schema: Optional Avro schema (auto-registered if provided)
            subject: Optional explicit subject name
            
        Returns:
            bool: True if publish was successful, False otherwise
        """
        await self._ensure_started()
        
        try:
            subject_name = self._get_subject(topic, subject)
            
            # Get or create serializer
            if subject_name not in self._serializers:
                if schema is None:
                    # Try to get existing schema from registry
                    try:
                        metadata = await self._schema_registry.get_latest_schema(
                            subject_name
                        )
                        schema = metadata.schema
                    except Exception:
                        raise ValueError(
                            f"No schema registered for subject '{subject_name}' "
                            "and no schema provided"
                        )
                
                # Register schema and create serializer
                await self.register_schema(topic, schema, subject)
            
            serializer = self._serializers[subject_name]
            
            # Serialize message to Avro
            serialized_value = await serializer.serialize(message, key)
            
            # Publish to Kafka
            await self._producer.send_and_wait(
                topic=topic,
                value=serialized_value,
                key=key
            )
            
            logger.debug(f"Published Avro message to topic '{topic}'")
            return True
        
        except KafkaError as e:
            logger.error(f"Kafka error publishing to '{topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing to '{topic}': {e}")
            return False
    
    async def publish_batch(
        self,
        topic: str,
        messages: list[dict[str, Any]],
        keys: Optional[list[str]] = None,
        schema: Optional[str | dict] = None,
        subject: Optional[str] = None
    ) -> bool:
        """
        Publish multiple messages to Kafka with Avro serialization.
        
        Args:
            topic: Kafka topic to publish to
            messages: List of message payloads
            keys: Optional list of message keys
            schema: Optional Avro schema
            subject: Optional explicit subject name
            
        Returns:
            bool: True if all publishes were successful, False otherwise
        """
        await self._ensure_started()
        
        if keys and len(keys) != len(messages):
            logger.error("Keys length must match messages length")
            return False
        
        try:
            subject_name = self._get_subject(topic, subject)
            
            # Ensure serializer exists
            if subject_name not in self._serializers:
                if schema is None:
                    metadata = await self._schema_registry.get_latest_schema(
                        subject_name
                    )
                    schema = metadata.schema
                await self.register_schema(topic, schema, subject)
            
            serializer = self._serializers[subject_name]
            
            # Serialize and send all messages
            futures = []
            for i, message in enumerate(messages):
                key = keys[i] if keys else None
                serialized_value = await serializer.serialize(message, key)
                
                future = await self._producer.send(
                    topic=topic,
                    value=serialized_value,
                    key=key
                )
                futures.append(future)
            
            # Wait for all messages to be sent
            for future in futures:
                await future
            
            logger.debug(f"Published {len(messages)} Avro messages to topic '{topic}'")
            return True
        
        except KafkaError as e:
            logger.error(f"Kafka error in batch publish to '{topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in batch publish to '{topic}': {e}")
            return False
    
    async def close(self) -> None:
        """Close the Kafka producer and Schema Registry client."""
        if self._producer and self._started:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        
        if self._schema_registry:
            try:
                await self._schema_registry.close()
            except Exception as e:
                logger.error(f"Error closing Schema Registry client: {e}")
        
        self._started = False
        self._serializers.clear()
        logger.info("Avro Kafka producer closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
