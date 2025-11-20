"""
Memorial API Client for fetching memorial information.

This module provides a client to interact with the memorial API
to retrieve recent memorial visits.
"""

import logging
from typing import Optional
import httpx
from core.exceptions import APIClientException


logger = logging.getLogger(__name__)


class MemorialAPIClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)

        logger.info(f"Memorial API Client initialized: {self.base_url}")

    async def get_recent_memorials(self, user_id: str, days: int = 7) -> Optional[list[dict]]:
        """
        Fetch recent memorial visits for a user.

        Args:
            user_id: User ID to fetch memorials for
            days: Number of days to look back

        Returns:
            List of recent memorials or empty list if none found

        Raises:
            APIClientException: If API call fails
        """
        try:
            url = f"{self.base_url}/memorials/tracing/recent"
            headers = {"user-id": user_id}
            params = {"day": days}

            logger.info(f"Fetching recent memorials: user_id={user_id}, days={days}")

            response = await self.client.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            memorial_data = data.get('data', [])

            if not memorial_data:
                logger.warning(f"No recent memorials found for user {user_id}")
                return []

            logger.info(
                f"Successfully fetched {len(memorial_data)} recent memorials for user {user_id}"
            )

            return memorial_data

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error fetching recent memorials for user {user_id}: "
                f"status={e.response.status_code}"
            )
            raise APIClientException(
                "Memorial API",
                e.response.status_code,
                f"추모관 조회 실패 (userId={user_id})"
            )

        except httpx.RequestError as e:
            logger.error(f"Request error fetching recent memorials for user {user_id}: {e}")
            raise APIClientException(
                "Memorial API",
                500,
                f"네트워크 오류: {e}"
            )

        except Exception as e:
            logger.error(f"Unexpected error fetching recent memorials for user {user_id}: {e}")
            raise APIClientException(
                "Memorial API",
                500,
                f"예상치 못한 오류: {e}"
            )

    async def close(self) -> None:
        await self.client.aclose()
        logger.info("Memorial API Client closed")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
