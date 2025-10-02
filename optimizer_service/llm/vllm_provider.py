import json
from openai import OpenAI
from .base_provider import BaseLLMProvider


class VLLMProvider(BaseLLMProvider):
    """
    Реализация провайдера для локального сервера vLLM.
    Использует OpenAI-совместимый API.
    """

    def __init__(self, host: str = "vllm", port: int = 8000, model_name: str = "casperhansen/sqlcoder-7b-2-awq"):
        self.client = OpenAI(
            base_url=f"http://{host}:{port}/v1",
            api_key="dummy-key"
        )
        self.model_name = model_name
        print(f"Инициализирован VLLMProvider для модели: {self.model_name} на http://{host}:{port}")

    def get_completion(self, prompt: str) -> dict:
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=8192,
            )

            raw_text = response.choices[0].message.content

            if raw_text.strip().startswith("```json"):
                json_part = raw_text[raw_text.find("```json") + 7: raw_text.rfind("```")]
            elif "{" in raw_text and "}" in raw_text:
                json_part = raw_text[raw_text.find("{"): raw_text.rfind("}") + 1]
            else:
                raise ValueError("В ответе vLLM не найден JSON-объект.")

            return json.loads(json_part)
        except Exception as e:
            print(f"Ошибка при вызове локального vLLM API: {e}")
            raise