import requests
from typing import Optional, Dict, Any


class HttpUtil:
    def __init__(self, default_timeout: int = 5):
        self.default_timeout = default_timeout

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        try:
            res = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout or self.default_timeout
            )

            res.raise_for_status()

            try:
                return {"success": True, "data": res.json(), "status": res.status_code}
            except ValueError:
                return {"success": True, "data": res.text, "status": res.status_code}

        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}


http = HttpUtil()
