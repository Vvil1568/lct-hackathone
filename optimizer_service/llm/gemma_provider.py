import time
import requests
import json
from .base_provider import BaseLLMProvider
from optimizer_service.core.config import settings

class GemmaAPIProvider(BaseLLMProvider):
    """
    Реализация провайдера для облачного API Google Gemma.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.model_name = "gemma-3-27b-it"
        self.api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent"

    def get_completion(self, prompt: str) -> dict:
        """
        Отправляет промпт в API и возвращает ответ в виде словаря.
        Реализует логику повторных попыток с экспоненциальной выдержкой.
        """
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}

        max_retries = 3
        base_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.api_url,
                    params={"key": self.api_key},
                    headers=headers,
                    json=payload,
                    timeout=180
                )
                response.raise_for_status()

                raw_text = response.json()['candidates'][0]['content']['parts'][0]['text']
                if raw_text.strip().startswith("```json"):
                    raw_text = raw_text.strip()[7:-3]

                return json.loads(raw_text)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    delay = base_delay * (2 ** attempt)
                    print(f"Получен статус 429 (Too Many Requests). Повторная попытка через {delay} сек...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"HTTP ошибка при вызове LLM API: {e}")
                    raise
            except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Ошибка при вызове или парсинге ответа LLM: {e}")
                raise

        raise Exception("Не удалось получить ответ от LLM API после нескольких попыток.")

llm_provider = GemmaAPIProvider(api_key=settings.GEMMA_API_KEY)