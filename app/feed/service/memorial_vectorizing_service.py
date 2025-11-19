"""
Memorial Vectorizing Service.

This service handles the complete flow of processing memorial vectorizing requests:
1. Fetch character information from API
2. Combine memorial content with character data
3. Generate embeddings
4. Store vectors in Pinecone
"""

import logging
from typing import Optional
import os
from dotenv import load_dotenv
from core.embedder import Embedder
from core.vectorstores import PineconeVectorStore
from core.clients import CharacterAPIClient

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


    async def process_memorial(self, memorial_data: dict) -> bool:
        try:
            memorial_id = memorial_data.get('memorialId')
            writer_id = memorial_data.get('writerId')
            content = memorial_data.get('content')
            character_id = memorial_data.get('characterId')



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

            # 5. Pincone에 저장
            success = self._store_vector(memorial_id, embedding, metadata)

            if success:
                return True
            else:
                return False

        except Exception as e:
            return False

    async def _fetch_character_info(self, character_id: int) -> Optional[dict]:
        try:
            character_data = await self.character_client.get_character(character_id)

            if not character_data:
                return None

            filtered_data = self.character_client.filter_character_data(character_data)

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
            vector_id = f"memorial-{memorial_id}"

            self.vectorstore.upsert(
                id=vector_id,
                embedding=embedding,
                metadata=metadata
            )
            return True

        except Exception as e:
            return False

    async def delete_memorial(self, memorial_id: int) -> bool:
        try:
            vector_id = f"memorial-{memorial_id}"

            self.vectorstore.delete(vector_id)

            return True

        except Exception as e:
            return False
