from abc import ABC, abstractmethod
from typing import List, Dict
from optimizer_service.models.schemas import ProfiledQuery

class DetectionResult:
    """Структура для хранения результатов работы детектора."""
    def __init__(self, pattern_name: str, message: str, priority: int, queries: List[ProfiledQuery]):
        self.pattern_name = pattern_name
        self.message = message
        self.priority = priority
        self.queries = queries

class BasePatternDetector(ABC):
    """Абстрактный базовый класс для всех детекторов."""
    @abstractmethod
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        """Запускает анализ и возвращает список найденных проблем."""
        pass