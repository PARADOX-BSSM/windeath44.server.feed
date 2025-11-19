"""
Avro serializer and deserializer for Kafka messages.

This module provides serialization and deserialization functionality
for Avro-encoded Kafka messages with Schema Registry integration.
"""

import io
import struct
import logging
from typing import Any, Optional
import avro.schema
import avro.io
from .schema_registry_client import SchemaRegistryClient


logger = logging.getLogger(__name__)


class AvroSerializer:
    """
    Serializes Python dictionaries to Avro binary format.

    Integrates with Schema Registry to manage schemas and includes
    schema ID in the serialized payload (Confluent wire format).
    """

    # Confluent wire format: magic byte (0x00) + 4-byte schema ID + Avro data
    MAGIC_BYTE = 0
    
    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        subject: str,
        schema: Optional[str | dict] = None,
        auto_register: bool = True
    ):
        """
        Initialize Avro serializer.

        Args:
            schema_registry_client: Schema Registry client
            subject: Subject name for schema registration
            schema: Avro schema (will be registered if auto_register=True)
            auto_register: Whether to auto-register schema
        """
        self.schema_registry_client = schema_registry_client
        self.subject = subject
        self.schema_str = schema
        self.auto_register = auto_register
        self._schema_id: Optional[int] = None
        self._avro_schema: Optional[avro.schema.Schema] = None
    
    async def _ensure_schema_registered(self):
        """Ensure schema is registered and cached."""
        if self._schema_id is not None:
            return
        
        if self.schema_str is None:
            # Try to get latest schema from registry
            try:
                metadata = await self.schema_registry_client.get_latest_schema(
                    self.subject
                )
                self.schema_str = metadata.schema
                self._schema_id = metadata.schema_id
                logger.info(f"Loaded schema from registry: ID {self._schema_id}")
            except Exception as e:
                raise ValueError(
                    f"No schema provided and failed to load from registry: {e}"
                )
        else:
            # Register the schema
            if self.auto_register:
                self._schema_id = await self.schema_registry_client.register_schema(
                    self.subject,
                    self.schema_str
                )
                logger.info(f"Registered schema: ID {self._schema_id}")
            else:
                raise ValueError("Schema not registered and auto_register is False")
        
        # Parse the schema
        if isinstance(self.schema_str, dict):
            import json
            schema_json = json.dumps(self.schema_str)
        else:
            schema_json = self.schema_str
        
        self._avro_schema = avro.schema.parse(schema_json)
    
    async def serialize(
        self,
        data: dict[str, Any],
        key: Optional[str] = None
    ) -> bytes:
        """
        Serialize data to Avro binary format with Confluent wire format.

        Args:
            data: Data to serialize
            key: Optional message key (not serialized, just for logging)

        Returns:
            Serialized bytes with schema ID prefix
        """
        await self._ensure_schema_registered()
        
        try:
            # Serialize to Avro
            writer = avro.io.DatumWriter(self._avro_schema)
            bytes_writer = io.BytesIO()
            
            # Write Confluent wire format header
            # Magic byte (1 byte) + Schema ID (4 bytes, big-endian)
            bytes_writer.write(struct.pack('b', self.MAGIC_BYTE))
            bytes_writer.write(struct.pack('>I', self._schema_id))
            
            # Write Avro data
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            
            serialized = bytes_writer.getvalue()
            logger.debug(f"Serialized message: {len(serialized)} bytes")
            return serialized
        
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    
    def __call__(self, data: dict[str, Any], ctx=None) -> bytes:
        """
        Synchronous serialization interface for compatibility.

        Note: This requires the schema to already be registered.
        For async contexts, use serialize() instead.
        """
        if self._schema_id is None or self._avro_schema is None:
            raise RuntimeError(
                "Schema not initialized. Call serialize() first in async context."
            )
        
        try:
            writer = avro.io.DatumWriter(self._avro_schema)
            bytes_writer = io.BytesIO()
            
            bytes_writer.write(struct.pack('b', self.MAGIC_BYTE))
            bytes_writer.write(struct.pack('>I', self._schema_id))
            
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            
            return bytes_writer.getvalue()
        
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise


class AvroDeserializer:
    """
    Deserializes Avro binary format to Python dictionaries.

    Uses Schema Registry to fetch schemas by ID from the message payload.
    """

    MAGIC_BYTE = 0
    
    def __init__(self, schema_registry_client: SchemaRegistryClient):
        """
        Initialize Avro deserializer.

        Args:
            schema_registry_client: Schema Registry client
        """
        self.schema_registry_client = schema_registry_client
        self._schema_cache: dict[int, avro.schema.Schema] = {}
    
    async def deserialize(
        self,
        data: bytes,
        key: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Deserialize Avro binary data to Python dictionary.

        Args:
            data: Serialized Avro data with Confluent wire format
            key: Optional message key (not used, just for logging)

        Returns:
            Deserialized data as dictionary
        """
        if data is None or len(data) == 0:
            return None
        
        try:
            bytes_reader = io.BytesIO(data)
            
            # Read Confluent wire format header
            magic_byte = struct.unpack('b', bytes_reader.read(1))[0]
            if magic_byte != self.MAGIC_BYTE:
                raise ValueError(f"Invalid magic byte: {magic_byte}")
            
            schema_id = struct.unpack('>I', bytes_reader.read(4))[0]
            
            # Get schema from cache or registry
            if schema_id not in self._schema_cache:
                schema_str = await self.schema_registry_client.get_schema_by_id(
                    schema_id
                )
                self._schema_cache[schema_id] = avro.schema.parse(schema_str)
            
            avro_schema = self._schema_cache[schema_id]
            
            # Deserialize Avro data
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(avro_schema)
            result = reader.read(decoder)
            
            logger.debug(f"Deserialized message using schema ID {schema_id}")
            return result
        
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
    
    def __call__(self, data: bytes, ctx=None) -> dict[str, Any]:
        """
        Synchronous deserialization interface.
        
        Note: This requires schemas to be cached. For first-time deserialization,
        use deserialize() in async context.
        """
        if data is None or len(data) == 0:
            return None
        
        try:
            bytes_reader = io.BytesIO(data)
            
            magic_byte = struct.unpack('b', bytes_reader.read(1))[0]
            if magic_byte != self.MAGIC_BYTE:
                raise ValueError(f"Invalid magic byte: {magic_byte}")
            
            schema_id = struct.unpack('>I', bytes_reader.read(4))[0]
            
            if schema_id not in self._schema_cache:
                raise RuntimeError(
                    f"Schema {schema_id} not in cache. Use deserialize() in async context."
                )
            
            avro_schema = self._schema_cache[schema_id]
            
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(avro_schema)
            return reader.read(decoder)
        
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
