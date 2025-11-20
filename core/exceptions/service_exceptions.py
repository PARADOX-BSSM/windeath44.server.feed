from .business_exception import BusinessException

class CharacterFetchException(BusinessException):
    """캐릭터 정보를 가져올 수 없을 때 발생하는 예외"""

    def __init__(self, character_id: int, reason: str = "캐릭터 정보를 가져올 수 없습니다"):
        self.character_id = character_id
        self.reason = reason
        super().__init__(
            status_code=404,
            message=f"캐릭터 정보 조회 실패 (characterId={character_id}): {reason}"
        )


class VectorStoreException(BusinessException):
    """벡터 저장소 작업 중 발생하는 예외"""

    def __init__(self, operation: str, reason: str):
        self.operation = operation
        self.reason = reason
        super().__init__(
            status_code=500,
            message=f"벡터 저장소 {operation} 실패: {reason}"
        )


class VectorNotFoundException(BusinessException):
    """벡터를 찾을 수 없을 때 발생하는 예외"""

    def __init__(self, vector_id: str):
        self.vector_id = vector_id
        super().__init__(
            status_code=404,
            message=f"벡터를 찾을 수 없습니다 (vectorId={vector_id})"
        )


class EmbeddingException(BusinessException):
    """임베딩 생성 중 발생하는 예외"""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(
            status_code=500,
            message=f"임베딩 생성 실패: {reason}"
        )


class MemorialDataException(BusinessException):
    """추모관 데이터가 유효하지 않을 때 발생하는 예외"""

    def __init__(self, field: str, reason: str = "필수 필드가 누락되었습니다"):
        self.field = field
        self.reason = reason
        super().__init__(
            status_code=400,
            message=f"추모관 데이터 오류 (field={field}): {reason}"
        )


class PublisherException(BusinessException):
    """Kafka 이벤트 발행 중 발생하는 예외"""

    def __init__(self, topic: str, reason: str):
        self.topic = topic
        self.reason = reason
        super().__init__(
            status_code=500,
            message=f"이벤트 발행 실패 (topic={topic}): {reason}"
        )


class SchemaException(BusinessException):
    """Avro 스키마 관련 예외"""

    def __init__(self, schema_name: str, reason: str):
        self.schema_name = schema_name
        self.reason = reason
        super().__init__(
            status_code=500,
            message=f"스키마 오류 (schema={schema_name}): {reason}"
        )


class APIClientException(BusinessException):
    """외부 API 호출 중 발생하는 예외"""

    def __init__(self, api_name: str, status_code: int, reason: str):
        self.api_name = api_name
        self.http_status_code = status_code
        self.reason = reason
        super().__init__(
            status_code=status_code if 400 <= status_code < 600 else 500,
            message=f"{api_name} API 호출 실패 (status={status_code}): {reason}"
        )


class MemorialNotFoundException(BusinessException):
    """추모관을 찾을 수 없을 때 발생하는 예외"""

    def __init__(self, user_id: str, reason: str = "최근 방문한 추모관이 없습니다"):
        self.user_id = user_id
        self.reason = reason
        super().__init__(
            status_code=404,
            message=f"추모관 조회 실패 (userId={user_id}): {reason}"
        )


class SearchException(BusinessException):
    """검색 작업 중 발생하는 예외"""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(
            status_code=500,
            message=f"검색 실패: {reason}"
        )


class EmptyEmbeddingListException(BusinessException):
    """임베딩 리스트가 비어있을 때 발생하는 예외"""

    def __init__(self):
        super().__init__(
            status_code=400,
            message="임베딩 리스트가 비어있어 평균을 계산할 수 없습니다"
        )
