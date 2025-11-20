import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from starlette.responses import JSONResponse

from core.exceptions import BusinessException
from core.listener import MemorialVectorListener, MemorialDeleteListener
from app.feed.service import MemorialVectorStoreService, MemorialVectorDeleteService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



memorial_listener: MemorialVectorListener | None = None
memorial_delete_listener: MemorialDeleteListener | None = None
listener_task: asyncio.Task | None = None
delete_listener_task: asyncio.Task | None = None

memorial_store_service: MemorialVectorStoreService | None = None
memorial_delete_service: MemorialVectorDeleteService | None = None


async def process_memorial_message(data: dict) -> None:
    try:
        await memorial_store_service.process_memorial(data)
    except Exception as e:
        logger.error(f"Error processing memorial message: {e}", exc_info=True)
        raise


async def process_memorial_delete_message(data: dict) -> None:
    try:
        await memorial_delete_service.delete_memorial(data)
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

        memorial_listener = MemorialVectorListener(
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
    global listener_task, delete_listener_task, memorial_store_service, memorial_delete_service

    logger.info("Starting Feed Service")

    try:
        # Initialize services
        memorial_store_service = MemorialVectorStoreService()
        logger.info("Memorial Vector Store Service initialized")

        memorial_delete_service = MemorialVectorDeleteService()
        logger.info("Memorial Vector Delete Service initialized")

        # Initialize publishers
        await memorial_store_service.initialize_publisher()
        logger.info("Store service publisher initialized")

        await memorial_delete_service.initialize_publisher()
        logger.info("Delete service publisher initialized")

        # Start listeners
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
    
    # Close publishers
    if memorial_store_service:
        await memorial_store_service.close_publisher()
        logger.info("Store service publisher closed")
    
    if memorial_delete_service:
        await memorial_delete_service.close_publisher()
        logger.info("Delete service publisher closed")
app = FastAPI(
    title="Feed Service",
    description="Memorial vectorizing service with Kafka integration",
    version="0.1.1",
    lifespan=lifespan
)

@app.exception_handler(Exception)
async def global_exception_handler(request : Request, exc: Exception):
    if isinstance(exc, BusinessException):
        print("error:", exc.message)
        print("status code:", exc.status_code)
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "message": exc.message,
                "status": exc.status_code
            },
        )
    print("error:", str(exc))
    return JSONResponse(status_code=500, content={
                "message": str(exc),
                "status": 500
            })


# Include routers
from api.routers.feed_router import router as feed_router
app.include_router(feed_router)


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
