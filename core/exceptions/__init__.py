"""
Custom exceptions for the Feed service.
"""

from .business_exception import BusinessException
from .service_exceptions import (
    CharacterFetchException,
    VectorStoreException,
    VectorNotFoundException,
    EmbeddingException,
    MemorialDataException,
    PublisherException,
    SchemaException,
    APIClientException,
    MemorialNotFoundException,
    SearchException,
    EmptyEmbeddingListException,
)

__all__ = [
    "BusinessException",
    "CharacterFetchException",
    "VectorStoreException",
    "VectorNotFoundException",
    "EmbeddingException",
    "MemorialDataException",
    "PublisherException",
    "SchemaException",
    "APIClientException",
    "MemorialNotFoundException",
    "SearchException",
    "EmptyEmbeddingListException",
]
