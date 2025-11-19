from typing import Protocol, Any, Optional
from abc import abstractmethod


class Publisher(Protocol):
    @abstractmethod
    async def publish(
        self, 
        topic: str, 
        message: dict[str, Any],
        key: Optional[str] = None
    ) -> bool:
        """
        Publish a message to a topic.

        Args:
            topic: The topic to publish to
            message: The message payload as a dictionary
            key: Optional message key for partitioning

        Returns:
            bool: True if publish was successful, False otherwise
        """
        ...
    
    @abstractmethod
    async def close(self) -> None:
        """
        Close the publisher and cleanup resources.
        """
        ...
