from abc import ABC, abstractmethod

class BaseLLMProvider(ABC):
    """
    Абстрактный базовый класс для всех LLM-провайдеров.
    Определяет единый интерфейс для взаимодействия с языковыми моделями.
    """
    @abstractmethod
    def get_completion(self, prompt: str) -> dict:
        """
        Отправляет промпт в API и возвращает ответ в виде словаря.
        """
        pass