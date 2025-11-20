"""
Feed Search Service for retrieving personalized memorial feeds.

This service handles the logic for:
1. Fetching recent memorial visits
2. Vectorizing memorial data
3. Performing similarity search
"""

import logging
import os
from typing import Optional
from dotenv import load_dotenv
from core.clients import MemorialAPIClient
from core.embedder import Embedder
from core.vectorstores import PineconeVectorStore
from core.exceptions import (
    MemorialNotFoundException,
    SearchException,
    EmptyEmbeddingListException,
)

load_dotenv()

logger = logging.getLogger(__name__)


class FeedSearchService:
    def __init__(self):
        memorial_api_url = os.getenv(
            "MEMORIAL_API_URL",
        )

        self.memorial_client = MemorialAPIClient(base_url=memorial_api_url)
        self.embedder = Embedder()
        self.vector_store = PineconeVectorStore()
        
        logger.info("Feed Search Service initialized")

    async def search_feeds(
        self,
        user_id: str,
        days: int = 7,
        top_k: int = 10
    ) -> dict:
        try:
            logger.info(f"Fetching recent memorial visits for user {user_id}")
            recent_visits = await self.memorial_client.get_recent_memorials(
                user_id=user_id,
                days=days
            )

            if recent_visits is None:
                raise MemorialNotFoundException(user_id, "API 호출 실패")

            if not recent_visits:
                logger.info(f"No recent memorial visits found for user {user_id}")
                return {
                    "user_id": user_id,
                    "recent_memorials": [],
                    "search_results": {"matches": []}
                }

            memorial_ids = self._extract_memorial_ids(recent_visits)

            if not memorial_ids:
                logger.warning("No valid memorial IDs found in visit records")
                return {
                    "user_id": user_id,
                    "recent_memorials": [],
                    "search_results": {"matches": []}
                }

            logger.info(f"Fetching {len(memorial_ids)} memorials by IDs")
            recent_memorials = await self.memorial_client.get_memorials_by_ids(memorial_ids)

            if recent_memorials is None:
                raise MemorialNotFoundException(user_id, "추모관 데이터 조회 실패")

            if not recent_memorials:
                logger.warning("No memorial data found for the IDs")
                return {
                    "user_id": user_id,
                    "recent_memorials": [],
                    "search_results": {"matches": []}
                }

            logger.info(f"Vectorizing {len(recent_memorials)} recent memorials")
            memorial_texts = self._prepare_memorial_texts(recent_memorials)

            if not memorial_texts:
                logger.warning("No valid memorial texts to vectorize")
                return {
                    "user_id": user_id,
                    "recent_memorials": recent_memorials,
                    "search_results": {"matches": []}
                }


            embeddings = self.embedder.embed_texts(memorial_texts)
            avg_embedding = self._average_embeddings(embeddings)

            logger.info(f"Searching for top {top_k} similar memorials")
            try:
                search_results = self.vector_store.query(
                    vector=avg_embedding,
                    top_k=top_k,
                    include_metadata=True,
                    include_values=False
                )
            except Exception as e:
                raise SearchException(f"벡터 검색 실패: {e}")

            return {
                "user_id": user_id,
                "recent_memorials": recent_memorials,
                "search_results": search_results
            }

        except (MemorialNotFoundException, SearchException, EmptyEmbeddingListException) as e:
            logger.error(f"Business exception searching feeds: {e.message}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error searching feeds for user {user_id}: {e}", exc_info=True)
            raise SearchException(str(e))

    def _extract_memorial_ids(self, visit_records: list[dict]) -> list[int]:
        memorial_ids = []
        for record in visit_records:
            memorial_id = record.get('memorialId')
            if memorial_id is not None:
                memorial_ids.append(memorial_id)
            else:
                logger.warning(f"Visit record missing memorialId: {record}")

        unique_ids = list(dict.fromkeys(memorial_ids))
        logger.info(f"Extracted {len(unique_ids)} unique memorial IDs from {len(visit_records)} visit records")

        return unique_ids

    def _prepare_memorial_texts(self, memorials: list[dict]) -> list[str]:
        texts = []
        
        for memorial in memorials:
            parts = []

            if 'name' in memorial:
                parts.append(f"Name: {memorial['name']}")
            if 'description' in memorial:
                parts.append(f"Description: {memorial['description']}")
            if 'content' in memorial:
                parts.append(f"Content: {memorial['content']}")
            if 'tags' in memorial:
                tags = memorial['tags']
                if isinstance(tags, list):
                    parts.append(f"Tags: {', '.join(str(tag) for tag in tags)}")
                else:
                    parts.append(f"Tags: {tags}")
            
            if parts:
                texts.append(" | ".join(parts))
        
        return texts

    def _average_embeddings(self, embeddings: list[list[float]]) -> list[float]:
        if not embeddings:
            raise EmptyEmbeddingListException()

        num_embeddings = len(embeddings)
        embedding_dim = len(embeddings[0])

        avg_embedding = [0.0] * embedding_dim

        for embedding in embeddings:
            for i, value in enumerate(embedding):
                avg_embedding[i] += value

        avg_embedding = [value / num_embeddings for value in avg_embedding]

        return avg_embedding

    async def close(self):
        await self.memorial_client.close()
        logger.info("Feed Search Service closed")
