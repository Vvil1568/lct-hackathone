from collections import Counter
from itertools import combinations
from typing import List, Dict
from .base_detector import BasePatternDetector
from optimizer_service.models.schemas import ProfiledQuery, DetectionResult


class JoinPatternDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        join_pairs = Counter()
        for query in queries:
            pairs = combinations(sorted(query.tables), 2)
            join_pairs.update(pairs)

        top_joins = join_pairs.most_common(3)

        if not top_joins or top_joins[0][1] < 2:
            return []

        join_str = ", ".join([f"'{t[0][0]}' и '{t[0][1]}'" for t in top_joins])
        message = (
            f"Обнаружен паттерн 'Частые Соединения'. "
            f"Таблицы {join_str} постоянно соединяются вместе в самых дорогих запросах. "
            f"Стратегическая рекомендация: Денормализовать эти таблицы в единую 'витрину данных'."
        )

        return [DetectionResult(
            pattern_name="Frequent Joins",
            message=message,
            priority=10,
            queries=queries,
            detector_name=self.__class__.__name__
        )]