import os
from typing import Optional, Any
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()

class PineconeVectorStore:
    def __init__(
        self,
        api_key: Optional[str] = None,
        index_name: Optional[str] = None,
        dimension: int = 3072,
        metric: str = "cosine",
        cloud: str = "aws",
        region: str = "us-east-1"
    ):
        self.api_key = api_key or os.getenv("PINECONE_API_KEY")
        self.index_name = index_name or os.getenv("PINECONE_INDEX_NAME")
        self.namespace = "memorial"
        self.dimension = dimension
        self.metric = metric
        self.cloud = cloud
        self.region = region

        if not self.api_key:
            raise ValueError("Pinecone API key is required")
        if not self.index_name:
            raise ValueError("Pinecone index name is required")

        self.pc = Pinecone(api_key=self.api_key)
        self.index = self.pc.Index(self.index_name)


    def upsert(
        self,
        id: str,
        embedding: list[float],
        metadata: Optional[dict[str, Any]] = None,
        namespace: Optional[str] = None
    ) -> dict:
        """
        사용 예시:
            >>> store.upsert(
            ...     id="memorial-123",
            ...     embedding=[0.1, 0.2, ...],
            ...     metadata={
            ...         "memorialId": 123,
            ...         "writerId": "user-456",
            ...         "content": "Memorial content...",
            ...         "characterId": 789
            ...     }
            ... )
        """
        try:
            ns = namespace or self.namespace

            vector = {
                "id": id,
                "values": embedding,
            }

            if metadata:
                vector["metadata"] = metadata

            response = self.index.upsert(
                vectors=[vector],
                namespace=ns
            )

            return response

        except Exception as e:
            raise

    def upsert_batch(
        self,
        vectors: list[dict[str, Any]],
        namespace: Optional[str] = None,
        batch_size: int = 100
    ) -> dict:
        """
        사용 예시:
            >>> vectors = [
            ...     {
            ...         "id": "memorial-1",
            ...         "values": [0.1, 0.2, ...],
            ...         "metadata": {"memorialId": 1, "content": "..."}
            ...     },
            ...     {
            ...         "id": "memorial-2",
            ...         "values": [0.3, 0.4, ...],
            ...         "metadata": {"memorialId": 2, "content": "..."}
            ...     }
            ... ]
            >>> store.upsert_batch(vectors)
        """
        try:
            ns = namespace or self.namespace
            total_vectors = len(vectors)

            # Process in batches
            for i in range(0, total_vectors, batch_size):
                batch = vectors[i:i + batch_size]
                self.index.upsert(
                    vectors=batch,
                    namespace=ns
                )

            return {"upserted_count": total_vectors}
        except Exception as e:
            raise

    def fetch(
        self,
        ids: str | list[str],
        namespace: Optional[str] = None
    ) -> dict:
        """
        사용 예시:
            >>> result = store.fetch("memorial-123")
            >>> exists = "memorial-123" in result.get("vectors", {})
        """
        try:
            ns = namespace or self.namespace
            
            if isinstance(ids, str):
                ids = [ids]
            
            response = self.index.fetch(
                ids=ids,
                namespace=ns
            )
            
            return response
        
        except Exception as e:
            raise

    def delete(
        self,
        ids: str | list[str],
        namespace: Optional[str] = None
    ) -> dict:
        try:
            ns = namespace or self.namespace

            if isinstance(ids, str):
                ids = [ids]

            response = self.index.delete(
                ids=ids,
                namespace=ns
            )

            return response

        except Exception as e:
            raise

    def delete_all(
        self,
        namespace: Optional[str] = None
    ) -> dict:
        try:
            ns = namespace or self.namespace

            response = self.index.delete(
                delete_all=True,
                namespace=ns
            )

            return response

        except Exception as e:
            raise

    def query(
        self,
        vector: list[float],
        top_k: int = 10,
        namespace: Optional[str] = None,
        include_metadata: bool = True,
        include_values: bool = False
    ) -> dict:
        try:
            ns = namespace or self.namespace
            
            response = self.index.query(
                vector=vector,
                top_k=top_k,
                namespace=ns,
                include_metadata=include_metadata,
                include_values=include_values
            )
            
            return response
        
        except Exception as e:
            raise
