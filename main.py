"""
Feed Service - FastAPI Application with Kafka Listener Integration.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from dotenv import load_dotenv

from core.listener import MemorialListener, MemorialDeleteListener
from app.feed.service import MemorialVectorizingService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


memorial_listener: MemorialListener | None = None
memorial_delete_listener: MemorialDeleteListener | None = None
listener_task: asyncio.Task | None = None
delete_listener_task: asyncio.Task | None = None

memorial_service: MemorialVectorizingService | None = None


async def process_memorial_message(data: dict) -> None:
    try:
        await memorial_service.process_memorial(data)
    except Exception as e:
        logger.error(f"Error processing memorial message: {e}", exc_info=True)
        raise


async def process_memorial_delete_message(data: dict) -> None:
    try:
        memorial_id = data.get('memorialId')
        if memorial_id:
            await memorial_service.delete_memorial(memorial_id)
            logger.info(f"Successfully deleted memorial vector: {memorial_id}")
        else:
            logger.warning("Received delete message without memorialId")
    except Exception as e:
        logger.error(f"Error processing memorial delete message: {e}", exc_info=True)
        raise


async def start_listener() -> None:
    global memorial_listener, listener_task

    try:
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        group_id = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'memorial-vectorizing-consumer-group')

        logger.info(f"Initializing Memorial Kafka Listener: {bootstrap_servers}")

        memorial_listener = MemorialListener(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

        memorial_listener.set_message_handler(process_memorial_message)

        await memorial_listener.start()

        logger.info("Starting to consume memorial vectorizing requests")
        await memorial_listener.consume()

    except asyncio.CancelledError:
        logger.info("Listener task cancelled")
        raise
    except Exception as e:
        logger.error(f"Error in listener: {e}", exc_info=True)
        raise


async def start_delete_listener() -> None:
    global memorial_delete_listener, delete_listener_task

    try:
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        group_id = os.getenv('KAFKA_DELETE_CONSUMER_GROUP_ID', 'memorial-vector-delete-consumer-group')

        logger.info(f"Initializing Memorial Delete Kafka Listener: {bootstrap_servers}")

        memorial_delete_listener = MemorialDeleteListener(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

        memorial_delete_listener.set_message_handler(process_memorial_delete_message)

        await memorial_delete_listener.start()

        logger.info("Starting to consume memorial vector delete requests")
        await memorial_delete_listener.consume()

    except asyncio.CancelledError:
        logger.info("Delete listener task cancelled")
        raise
    except Exception as e:
        logger.error(f"Error in delete listener: {e}", exc_info=True)
        raise


async def stop_listener() -> None:
    global memorial_listener, listener_task

    if listener_task:
        logger.info("Stopping listener task")
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass

    if memorial_listener:
        logger.info("Closing memorial listener")
        await memorial_listener.close()
        memorial_listener = None


async def stop_delete_listener() -> None:
    global memorial_delete_listener, delete_listener_task

    if delete_listener_task:
        logger.info("Stopping delete listener task")
        delete_listener_task.cancel()
        try:
            await delete_listener_task
        except asyncio.CancelledError:
            pass

    if memorial_delete_listener:
        logger.info("Closing memorial delete listener")
        await memorial_delete_listener.close()
        memorial_delete_listener = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global listener_task, delete_listener_task, memorial_service

    logger.info("Starting Feed Service")

    try:
        memorial_service = MemorialVectorizingService()
        logger.info("Memorial Vectorizing Service initialized")

        listener_task = asyncio.create_task(start_listener())
        logger.info("Kafka listener started")

        delete_listener_task = asyncio.create_task(start_delete_listener())
        logger.info("Kafka delete listener started")

    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)
        raise

    yield

    # Shutdown
    logger.info("Shutting down Feed Service")

    await stop_listener()
    await stop_delete_listener()
app = FastAPI(
    title="Feed Service",
    description="Memorial vectorizing service with Kafka integration",
    version="0.1.1",
    lifespan=lifespan
)


@app.get("/")
async def root():
    return {
        "service": "Feed Service",
        "version": "0.1.1",
        "status": "running"
    }


@app.get("/health")
async def health():
    listener_status = "running" if memorial_listener and memorial_listener._started else "stopped"
    delete_listener_status = "running" if memorial_delete_listener and memorial_delete_listener._started else "stopped"

    return {
        "status": "healthy",
        "listeners": [
            {
                "name": "memorial-vectorizing",
                "status": listener_status,
                "topic": "memorial-vectorizing-request"
            },
            {
                "name": "memorial-vector-delete",
                "status": delete_listener_status,
                "topic": "memorial-vector-delete-request"
            }
        ]
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
