"""
Schema Registry Client for managing Avro schemas.

This module provides a client for interacting with Confluent Schema Registry
to register, retrieve, and manage Avro schemas.
"""

import logging
from typing import Optional
import aiohttp
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class SchemaMetadata:
    """Metadata about a registered schema."""
    schema_id: int
    schema: str
    subject: str
    version: int


class SchemaRegistryClient:
    """
    Client for Confluent Schema Registry.
    
    Provides methods to register and retrieve schemas from Schema Registry.
    """
    
    def __init__(
        self,
        url: str,
        timeout: int = 30,
        auth: Optional[tuple[str, str]] = None
    ):
        """
        Initialize Schema Registry client.
        
        Args:
            url: Schema Registry URL (e.g., "http://localhost:8081")
            timeout: Request timeout in seconds
            auth: Optional (username, password) tuple for authentication
        """
        self.url = url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.auth = aiohttp.BasicAuth(*auth) if auth else None
        self._session: Optional[aiohttp.ClientSession] = None
        self._schema_cache: dict[str, SchemaMetadata] = {}
        self._id_cache: dict[int, str] = {}
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                auth=self.auth
            )
    
    async def close(self):
        """Close the client session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def register_schema(
        self,
        subject: str,
        schema: str | dict
    ) -> int:
        """
        Register a schema under a subject.
        
        Args:
            subject: Subject name (typically "{topic}-value" or "{topic}-key")
            schema: Avro schema as JSON string or dict
            
        Returns:
            Schema ID assigned by the registry
        """
        await self._ensure_session()
        
        # Convert dict to JSON string if needed
        if isinstance(schema, dict):
            import json
            schema_str = json.dumps(schema)
        else:
            schema_str = schema
        
        url = f"{self.url}/subjects/{subject}/versions"
        payload = {"schema": schema_str}
        
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    schema_id = data["id"]
                    
                    # Cache the schema
                    self._schema_cache[subject] = SchemaMetadata(
                        schema_id=schema_id,
                        schema=schema_str,
                        subject=subject,
                        version=data.get("version", -1)
                    )
                    self._id_cache[schema_id] = schema_str
                    
                    logger.info(f"Registered schema for subject '{subject}': ID {schema_id}")
                    return schema_id
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to register schema: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error registering schema: {e}")
            raise
    
    async def get_schema_by_id(self, schema_id: int) -> str:
        """
        Get schema by ID from registry.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema as JSON string
        """
        # Check cache first
        if schema_id in self._id_cache:
            return self._id_cache[schema_id]
        
        await self._ensure_session()
        url = f"{self.url}/schemas/ids/{schema_id}"
        
        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    schema = data["schema"]
                    self._id_cache[schema_id] = schema
                    return schema
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to get schema: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error getting schema by ID: {e}")
            raise
    
    async def get_latest_schema(self, subject: str) -> SchemaMetadata:
        """
        Get the latest schema version for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            SchemaMetadata with latest schema information
        """
        # Check cache first
        if subject in self._schema_cache:
            return self._schema_cache[subject]
        
        await self._ensure_session()
        url = f"{self.url}/subjects/{subject}/versions/latest"
        
        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    metadata = SchemaMetadata(
                        schema_id=data["id"],
                        schema=data["schema"],
                        subject=subject,
                        version=data["version"]
                    )
                    self._schema_cache[subject] = metadata
                    return metadata
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to get latest schema: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error getting latest schema: {e}")
            raise
    
    async def get_schema_version(
        self,
        subject: str,
        version: int
    ) -> SchemaMetadata:
        """
        Get a specific schema version for a subject.
        
        Args:
            subject: Subject name
            version: Version number
            
        Returns:
            SchemaMetadata for the specified version
        """
        await self._ensure_session()
        url = f"{self.url}/subjects/{subject}/versions/{version}"
        
        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return SchemaMetadata(
                        schema_id=data["id"],
                        schema=data["schema"],
                        subject=subject,
                        version=data["version"]
                    )
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to get schema version: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error getting schema version: {e}")
            raise
    
    async def check_compatibility(
        self,
        subject: str,
        schema: str | dict
    ) -> bool:
        """
        Check if a schema is compatible with the latest version.
        
        Args:
            subject: Subject name
            schema: Schema to check
            
        Returns:
            True if compatible, False otherwise
        """
        await self._ensure_session()
        
        if isinstance(schema, dict):
            import json
            schema_str = json.dumps(schema)
        else:
            schema_str = schema
        
        url = f"{self.url}/compatibility/subjects/{subject}/versions/latest"
        payload = {"schema": schema_str}
        
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("is_compatible", False)
                else:
                    logger.warning(f"Compatibility check failed: {response.status}")
                    return False
        
        except aiohttp.ClientError as e:
            logger.error(f"Error checking compatibility: {e}")
            return False
    
    async def list_subjects(self) -> list[str]:
        """
        List all registered subjects.
        
        Returns:
            List of subject names
        """
        await self._ensure_session()
        url = f"{self.url}/subjects"
        
        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to list subjects: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error listing subjects: {e}")
            raise
    
    async def delete_subject(self, subject: str) -> list[int]:
        """
        Delete a subject and all its versions.
        
        Args:
            subject: Subject name to delete
            
        Returns:
            List of deleted version numbers
        """
        await self._ensure_session()
        url = f"{self.url}/subjects/{subject}"
        
        try:
            async with self._session.delete(url) as response:
                if response.status == 200:
                    versions = await response.json()
                    # Clear from cache
                    self._schema_cache.pop(subject, None)
                    logger.info(f"Deleted subject '{subject}'")
                    return versions
                else:
                    error_msg = await response.text()
                    raise Exception(f"Failed to delete subject: {error_msg}")
        
        except aiohttp.ClientError as e:
            logger.error(f"Error deleting subject: {e}")
            raise
    
    def clear_cache(self):
        """Clear the local schema cache."""
        self._schema_cache.clear()
        self._id_cache.clear()
        logger.info("Schema cache cleared")
