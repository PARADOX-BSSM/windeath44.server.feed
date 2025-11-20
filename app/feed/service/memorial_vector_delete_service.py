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
from core.exceptions import (
    VectorStoreException,
    MemorialDataException,
    PublisherException,
    SchemaException,
)

load_dotenv()

logger = logging.getLogger(__name__)


class MemorialVectorDeleteService:
    def __init__(self):
        self.vectorstore = PineconeVectorStore()
        self.publisher: Optional[AvroKafkaPublisher] = None

    async def initialize_publisher(self) -> None:
        """Initialize the Kafka publisher for response events."""
        if self.publisher is None:
            self.publisher = AvroKafkaPublisher(
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

        Raises:
            MemorialDataException: If memorialId is missing
            VectorStoreException: If vector deletion fails
            PublisherException: If event publishing fails
        """
        try:
            memorial_id = memorial_data.get('memorialId')

            if not memorial_id:
                raise MemorialDataException('memorialId', '필수 필드가 누락되었습니다')

            vector_id = vector_id_generator.create_vector_id(memorial_id)

            # Delete from Pinecone
            try:
                self.vectorstore.delete(vector_id)
                logger.info(f"Deleted memorial vector: memorialId={memorial_id}, vectorId={vector_id}")
            except Exception as e:
                raise VectorStoreException("delete", str(e))

            # Publish memorial-vector-delete-response event
            await self._publish_delete_response(memorial_data)

            return True

        except (MemorialDataException, VectorStoreException, PublisherException) as e:
            logger.error(f"Business exception deleting memorial: {e.message}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting memorial: {e}", exc_info=True)
            raise VectorStoreException("delete_memorial", str(e))

    async def _publish_delete_response(self, memorial_data: dict) -> None:
        """
        Publish memorial-vector-delete-response event using MemorialAvroSchema.

        Args:
            memorial_data: The original memorial data

        Raises:
            PublisherException: If event publishing fails
            SchemaException: If schema loading fails
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

            try:
                with open(schema_path, 'r') as f:
                    schema = json.load(f)
            except FileNotFoundError:
                raise SchemaException("MemorialAvroSchema", "스키마 파일을 찾을 수 없습니다")
            except json.JSONDecodeError as e:
                raise SchemaException("MemorialAvroSchema", f"스키마 파싱 실패: {e}")

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

            if not success:
                raise PublisherException(
                    "memorial-vector-delete-response",
                    "이벤트 발행에 실패했습니다"
                )

            logger.info(
                f"Published memorial-vector-delete-response: "
                f"memorialId={memorial_data.get('memorialId')}"
            )

        except (PublisherException, SchemaException):
            raise
        except Exception as e:
            logger.error(f"Error publishing delete response: {e}", exc_info=True)
            raise PublisherException("memorial-vector-delete-response", str(e))
