from abc import ABC, abstractmethod
from typing import List, Dict
from optimizer_service.models.schemas import ProfiledQuery, DetectionResult

class BasePatternDetector(ABC):
    """Абстрактный базовый класс для всех детекторов."""
    @abstractmethod
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        """Запускает анализ и возвращает список найденных проблем."""
        pass