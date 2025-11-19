
import json
import logging
from typing import Optional, Callable, Awaitable
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from core.serializer import AvroDeserializer, SchemaRegistryClient


logger = logging.getLogger(__name__)


from typing import Optional

from .kafka_listener import KafkaListener


class MemorialVectorListener(KafkaListener):
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        schema_registry_url: str,
        group_id: str = "feed",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        schema_registry_auth: Optional[tuple[str, str]] = None,
        **kwargs
    ):
        super().__init__(
            topic="memorial-vectorizing-request",
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            schema_registry_auth=schema_registry_auth,
            **kwargs
        )
