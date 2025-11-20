import logging
from fastapi import APIRouter, Header, Query, HTTPException

from app.feed.service import FeedSearchService


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/feeds", tags=["feeds"])


@router.get("")
async def get_feeds(
    user_id: str = Header(..., alias="user-id"),
    days: int = Query(default=7, ge=1, le=30, description="Number of days to look back"),
    size: int = Query(default=10, ge=1, le=100, description="Number of results to return")
):
    try:
        logger.info(f"Getting feeds for user {user_id}, days={days}, top_k={size}")
        
        feed_service = FeedSearchService()
        
        try:
            result = await feed_service.search_feeds(
                user_id=user_id,
                days=days,
                top_k=size
            )
            
            if result is None:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to search feeds"
                )
            
            return {
                "status": "success",
                "data": result
            }
            
        finally:
            await feed_service.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_feeds endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
