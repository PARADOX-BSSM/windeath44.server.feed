"""
Character API Client for fetching character information.

This module provides a client to interact with the character API
to retrieve character details by ID.
"""

import logging
import os
from typing import Optional
import httpx


logger = logging.getLogger(__name__)


class CharacterAPIClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)

        logger.info(f"Character API Client initialized: {self.base_url}")

    async def get_character(self, character_id: int) -> Optional[dict]:
        """
        캐릭터 데이터:
            {
                "characterId": 0,
                "animeId": 0,
                "memorialCommitId": 0,
                "name": "string",
                "age": 0,
                "imageUrl": "string",
                "deathOfDay": "string",
                "deathReason": "string",
                "causeOfDeathDetails": "string",
                "saying": "string"
            }
        """
        try:
            url = f"{self.base_url}/animes/characters/{character_id}"
            logger.info(f"Fetching character data: character_id={character_id}")

            response = await self.client.get(url)
            response.raise_for_status()

            data = response.json()

            character_data = data.get('data')

            if not character_data:
                logger.warning(f"No character data found for ID {character_id}")
                return None

            logger.info(
                f"Successfully fetched character: "
                f"character_id={character_id}, name={character_data.get('name')}"
            )

            return character_data

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error fetching character {character_id}: "
                f"status={e.response.status_code}"
            )
            return None

        except httpx.RequestError as e:
            logger.error(f"Request error fetching character {character_id}: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error fetching character {character_id}: {e}")
            return None

    def filter_character_data(self, character_data: dict) -> dict:
        excluded_fields = {'imageUrl', 'memorialCommitId', 'deathOfDay'}

        filtered_data = {
            key: value
            for key, value in character_data.items()
            if key not in excluded_fields
        }

        logger.debug(f"Filtered character data: {filtered_data}")
        return filtered_data

    async def close(self) -> None:
        await self.client.aclose()
        logger.info("Character API Client closed")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
