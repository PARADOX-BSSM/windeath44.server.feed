"""
Memorial Vector Store Service.

This service handles the complete flow of processing memorial vectorizing requests:
1. Fetch character information from API
2. Combine memorial content with character data
3. Generate embeddings
4. Store vectors in Pinecone
5. Publish response events
"""

import logging
from typing import Optional, Literal
import os
import time
import json
from dotenv import load_dotenv
from core.embedder import Embedder
from core.util import vector_id_generator
from core.vectorstores import PineconeVectorStore
from core.clients import CharacterAPIClient
from core.publisher import AvroKafkaPublisher

load_dotenv()

logger = logging.getLogger(__name__)


class MemorialVectorStoreService:
    def __init__(self):
        self.embedder = Embedder()
        self.vectorstore = PineconeVectorStore()
        self.character_client = CharacterAPIClient(
            base_url=os.getenv("CHARACTER_API_BASE_URL")
        )
        self.publisher: Optional[AvroKafkaPublisher] = None

    async def initialize_publisher(self) -> None:
        """Initialize the Kafka publisher for response events."""
        if self.publisher is None:
            self.publisher = AvroKafkaPublisher(
                client_id="memorial-vector-store-response-publisher"
            )
            await self.publisher.start()
            logger.info("Memorial Vector Store publisher initialized")

    async def close_publisher(self) -> None:
        """Close the Kafka publisher."""
        if self.publisher:
            await self.publisher.close()
            self.publisher = None
            logger.info("Memorial Vector Store publisher closed")

    async def process_memorial(self, memorial_data: dict) -> bool:
        """
        Process memorial vectorizing request.
        
        Args:
            memorial_data: Memorial data from Kafka event
            
        Returns:
            bool: True if processing succeeded, False otherwise
        """
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
                logger.error(f"Failed to fetch character info for characterId={character_id}")
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
                logger.info(f"Successfully processed memorial: memorialId={memorial_id}, actionType={action_type}")
                return True
            else:
                logger.error(f"Failed to store vector for memorialId={memorial_id}")
                return False

        except Exception as e:
            logger.error(f"Error processing memorial: {e}", exc_info=True)
            return False

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
            logger.error(f"Error determining action type: {e}", exc_info=True)
            # Default to CREATE if we can't determine
            return "CREATE"

    async def _fetch_character_info(self, character_id: int) -> Optional[dict]:
        """
        Fetch and filter character information from the API.
        
        Args:
            character_id: The character ID to fetch
            
        Returns:
            Filtered character data or None if failed
        """
        try:
            character_data = await self.character_client.get_character(character_id)
            if not character_data:
                return None

            filtered_data = self.character_client.filter_character_data(character_data)
            return filtered_data

        except Exception as e:
            logger.error(f"Error fetching character info: {e}", exc_info=True)
            return None

    def _combine_memorial_and_character(
        self,
        memorial_content: str,
        character_data: dict
    ) -> str:
        """
        Combine memorial content with character information.
        
        Args:
            memorial_content: The memorial content text
            character_data: Character information dictionary
            
        Returns:
            Combined text for embedding
        """
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
        """
        Prepare metadata for vector storage.
        
        Args:
            memorial_data: Original memorial data
            character_data: Character information
            
        Returns:
            Metadata dictionary
        """
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
        """
        Store vector in Pinecone.
        
        Args:
            memorial_id: Memorial ID
            embedding: Vector embedding
            metadata: Metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            vector_id = vector_id_generator.create_vector_id(memorial_id)

            self.vectorstore.upsert(
                id=vector_id,
                embedding=embedding,
                metadata=metadata
            )
            return True

        except Exception as e:
            logger.error(f"Error storing vector: {e}", exc_info=True)
            return False

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
                logger.info(
                    f"Published memorial-vectorizing-response: "
                    f"memorialId={memorial_data.get('memorialId')}, actionType={action_type}"
                )
            else:
                logger.error("Failed to publish memorial-vectorizing-response")
                
        except Exception as e:
            logger.error(f"Error publishing response: {e}", exc_info=True)
