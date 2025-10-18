import requests
import json
from .base_provider import BaseLLMProvider


class LlamaCppProvider(BaseLLMProvider):
    """
    Реализация провайдера для локального сервера llama-cpp-python.
    """

    def __init__(self, host: str = "llama-cpp", port: int = 8000):
        self.api_url = f"http://{host}:{port}/completion"
        print(f"Инициализирован LlamaCppProvider на {self.api_url}")

    def get_completion(self, prompt: str) -> dict:
        headers = {"Content-Type": "application/json"}
        payload = {
            "prompt": prompt,
            "n_predict": 2048,
            "temperature": 0.1,
            "response_format": {
                "type": "json_object"
            }
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=300)
            response.raise_for_status()

            response_json = response.json()
            raw_text = response_json.get('content', '')

            if raw_text.strip().startswith("```json"):
                json_part = raw_text[raw_text.find("```json") + 7: raw_text.rfind("```")]
            elif "{" in raw_text and "}" in raw_text:
                json_part = raw_text[raw_text.find("{"): raw_text.rfind("}") + 1]
            else:
                raise ValueError("В ответе vLLM не найден JSON-объект.")

            return json.loads(json_part)
        except Exception as e:
            print(f"Ошибка при вызове локального llama-cpp API: {e}")
            raise