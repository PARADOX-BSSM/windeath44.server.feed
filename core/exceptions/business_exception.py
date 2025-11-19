class BusinessException(Exception):
    def __init__(self, message: str = "에러 발생", status_code: int = 500):
        self.status_code = status_code
        self.message = message
        super().__init__(self.message, self.status_code)
