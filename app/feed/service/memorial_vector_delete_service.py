"""
Memorial Vector Delete Service.

This service handles the deletion of memorial vectors from Pinecone
and publishes deletion response events.
"""

import logging
from typing import Optional
import os
import json
from dotenv import load_dotenv
from core.util import vector_id_generator
from core.vectorstores import PineconeVectorStore
from core.publisher import AvroKafkaPublisher

load_dotenv()

logger = logging.getLogger(__name__)


class MemorialVectorDeleteService:
    def __init__(self):
        self.vectorstore = PineconeVectorStore()
        self.publisher: Optional[AvroKafkaPublisher] = None

    async def initialize_publisher(self) -> None:
        """Initialize the Kafka publisher for response events."""
        if self.publisher is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
            
            self.publisher = AvroKafkaPublisher(
                bootstrap_servers=bootstrap_servers,
                schema_registry_url=schema_registry_url,
                client_id="memorial-vector-delete-response-publisher"
            )
            await self.publisher.start()
            logger.info("Memorial Vector Delete publisher initialized")

    async def close_publisher(self) -> None:
        """Close the Kafka publisher."""
        if self.publisher:
            await self.publisher.close()
            self.publisher = None
            logger.info("Memorial Vector Delete publisher closed")

    async def delete_memorial(self, memorial_data: dict) -> bool:
        """
        Delete memorial vector and publish response event.

        Args:
            memorial_data: The memorial data from the delete request

        Returns:
            bool: True if deletion and response publishing succeeded
        """
        try:
            memorial_id = memorial_data.get('memorialId')
            
            if not memorial_id:
                logger.error("Missing memorialId in delete request")
                return False

            vector_id = vector_id_generator.create_vector_id(memorial_id)

            # Delete from Pinecone
            self.vectorstore.delete(vector_id)
            logger.info(f"Deleted memorial vector: memorialId={memorial_id}, vectorId={vector_id}")

            # Publish memorial-vector-delete-response event
            await self._publish_delete_response(memorial_data)

            return True

        except Exception as e:
            logger.error(f"Error deleting memorial: {e}", exc_info=True)
            return False

    async def _publish_delete_response(self, memorial_data: dict) -> None:
        """
        Publish memorial-vector-delete-response event using MemorialAvroSchema.

        Args:
            memorial_data: The original memorial data
        """
        try:
            if self.publisher is None:
                await self.initialize_publisher()

            # Read MemorialAvroSchema
            schema_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                "avro",
                "MemorialAvroSchema.avsc"
            )

            with open(schema_path, 'r') as f:
                schema = json.load(f)

            # Prepare response message using MemorialAvroSchema
            response_message = {
                "memorialId": memorial_data.get("memorialId"),
                "writerId": memorial_data.get("writerId"),
                "content": memorial_data.get("content"),
                "characterId": memorial_data.get("characterId")
            }

            # Publish to memorial-vector-delete-response topic
            success = await self.publisher.publish(
                topic="memorial-vector-delete-response",
                message=response_message,
                key=str(memorial_data.get("memorialId")),
                schema=schema
            )

            if success:
                logger.info(
                    f"Published memorial-vector-delete-response: "
                    f"memorialId={memorial_data.get('memorialId')}"
                )
            else:
                logger.error("Failed to publish memorial-vector-delete-response")

        except Exception as e:
            logger.error(f"Error publishing delete response: {e}", exc_info=True)
