"""
Memorial Vectorizing Service.

This service handles the complete flow of processing memorial vectorizing requests:
1. Fetch character information from API
2. Combine memorial content with character data
3. Generate embeddings
4. Store vectors in Pinecone
"""

import logging
from typing import Optional, Literal
import os
import time
from dotenv import load_dotenv
from core.embedder import Embedder
from core.util import vector_id_generator
from core.vectorstores import PineconeVectorStore
from core.clients import CharacterAPIClient
from core.publisher import AvroKafkaPublisher

load_dotenv()

class MemorialVectorizingService:
    def __init__(
        self
    ):
        self.embedder = Embedder()

        self.vectorstore = PineconeVectorStore()

        self.character_client = CharacterAPIClient(
            base_url=os.getenv("CHARACTER_API_BASE_URL")
        )

        self.publisher: Optional[AvroKafkaPublisher] = None

    async def initialize_publisher(self) -> None:
        """Initialize the Kafka publisher for response events."""
        if self.publisher is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
            
            self.publisher = AvroKafkaPublisher(
                bootstrap_servers=bootstrap_servers,
                schema_registry_url=schema_registry_url,
                client_id="memorial-vectorizing-response-publisher"
            )
            await self.publisher.start()

    async def close_publisher(self) -> None:
        """Close the Kafka publisher."""
        if self.publisher:
            await self.publisher.close()
            self.publisher = None

    async def _determine_action_type(self, memorial_id: int) -> Literal["CREATE", "UPDATE"]:
        """
        Determine if this is a CREATE or UPDATE operation by checking Pinecone.
        
        Args:
            memorial_id: The memorial ID to check
            
        Returns:
            "CREATE" if vector doesn't exist, "UPDATE" if it does
        """
        try:
            vector_id = vector_id_generator.create_vector_id(memorial_id)
            result = self.vectorstore.fetch(vector_id)
            
            # Check if the vector exists in the response
            vectors = result.get("vectors", {})
            if vector_id in vectors:
                return "UPDATE"
            else:
                return "CREATE"
        except Exception as e:
            logging.error(f"Error determining action type: {e}", exc_info=True)
            # Default to CREATE if we can't determine
            return "CREATE"

    async def _publish_response(
        self,
        action_type: Literal["CREATE", "UPDATE"],
        memorial_data: dict
    ) -> None:
        """
        Publish memorial-vectorizing-response event.
        
        Args:
            action_type: "CREATE" or "UPDATE"
            memorial_data: The original memorial data
        """
        try:
            if self.publisher is None:
                await self.initialize_publisher()
            
            # Read FeedAvroSchema
            schema_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                "avro",
                "FeedAvroSchema.avsc"
            )
            
            with open(schema_path, 'r') as f:
                import json
                schema = json.load(f)
            
            # Prepare response message
            response_message = {
                "actionType": action_type,
                "memorialId": memorial_data.get("memorialId"),
                "writerId": memorial_data.get("writerId"),
                "content": memorial_data.get("content"),
                "characterId": memorial_data.get("characterId"),
                "timestamp": int(time.time() * 1000),  # Current time in milliseconds
                "metadata": None
            }
            
            # Publish to memorial-vectorizing-response topic
            success = await self.publisher.publish(
                topic="memorial-vectorizing-response",
                message=response_message,
                key=str(memorial_data.get("memorialId")),
                schema=schema
            )
            
            if success:
                logging.info(
                    f"Published memorial-vectorizing-response: "
                    f"memorialId={memorial_data.get('memorialId')}, actionType={action_type}"
                )
            else:
                logging.error("Failed to publish memorial-vectorizing-response")
                
        except Exception as e:
            logging.error(f"Error publishing response: {e}", exc_info=True)


    async def process_memorial(self, memorial_data: dict) -> bool:
        try:
            memorial_id = memorial_data.get('memorialId')
            writer_id = memorial_data.get('writerId')
            content = memorial_data.get('content')
            character_id = memorial_data.get('characterId')

            # 0. Check if vector exists (determine CREATE or UPDATE)
            action_type = await self._determine_action_type(memorial_id)

            # 1. 캐릭터 정보 불러오기
            character_data = await self._fetch_character_info(character_id)

            if not character_data:
                return False

            # 2. 추모관 정보, 캐릭터 정보 합치기
            combined_text = self._combine_memorial_and_character(
                content, character_data
            )

            # 3. 벡터화
            embedding = self.embedder.embed_text(combined_text)

            # 4. 메타데이터 생성
            metadata = self._prepare_metadata(
                memorial_data, character_data
            )

            # 5. Pinecone에 저장
            success = self._store_vector(memorial_id, embedding, metadata)

            if success:
                # 6. memorial-vectorizing-response 이벤트 발행
                await self._publish_response(
                    action_type=action_type,
                    memorial_data=memorial_data
                )
                return True
            else:
                return False

        except Exception as e:
            logging.error(f"Error processing memorial: {e}", exc_info=True)
            return False

    async def _fetch_character_info(self, character_id: int) -> Optional[dict]:
        try:
            character_data = await self.character_client.get_character(character_id)

            if not character_data:
                return None

            filtered_data = self.character_client.filter_character_data(character_data)
            return filtered_data

        except Exception as e:
            return None

    def _combine_memorial_and_character(
        self,
        memorial_content: str,
        character_data: dict
    ) -> str:
        character_info_parts = [
            f"{key}: {value}"
            for key, value in character_data.items()
            if value is not None
        ]
        character_info_text = ", ".join(character_info_parts)

        combined_text = (
            f"Memorial: {memorial_content}\n"
            f"Character: {character_info_text}"
        )

        return combined_text

    def _prepare_metadata(
        self,
        memorial_data: dict,
        character_data: dict
    ) -> dict:
        metadata = {
            "memorialId": memorial_data.get('memorialId'),
            "writerId": memorial_data.get('writerId'),
            "content": memorial_data.get('content'),
            "characterId": memorial_data.get('characterId'),
            "characterName": character_data.get('name'),
            "characterAge": character_data.get('age'),
            "animeId": character_data.get('animeId'),
            "deathReason": character_data.get('deathReason'),
            "causeOfDeathDetails": character_data.get('causeOfDeathDetails'),
            "saying": character_data.get('saying'),
        }

        metadata = {k: v for k, v in metadata.items() if v is not None}

        return metadata

    def _store_vector(
        self,
        memorial_id: int,
        embedding: list[float],
        metadata: dict
    ) -> bool:
        try:
            vector_id = vector_id_generator.create_vector_id(memorial_id)

            self.vectorstore.upsert(
                id=vector_id,
                embedding=embedding,
                metadata=metadata
            )
            return True

        except Exception as e:
            return False

    async def delete_memorial(self, memorial_id: int, memorial_data: dict) -> bool:
        """
        Delete memorial vector and publish response event.

        Args:
            memorial_id: The memorial ID to delete
            memorial_data: The original memorial data from the request

        Returns:
            bool: True if deletion and response publishing succeeded
        """
        try:
            vector_id = vector_id_generator.create_vector_id(memorial_id)

            self.vectorstore.delete(vector_id)

            # Publish memorial-vector-delete-response event
            await self._publish_delete_response(memorial_data)

            return True

        except Exception as e:
            logging.error(f"Error deleting memorial: {e}", exc_info=True)
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
                import json
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
                logging.info(
                    f"Published memorial-vector-delete-response: "
                    f"memorialId={memorial_data.get('memorialId')}"
                )
            else:
                logging.error("Failed to publish memorial-vector-delete-response")

        except Exception as e:
            logging.error(f"Error publishing delete response: {e}", exc_info=True)
