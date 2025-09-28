from typing import List, Dict
import sqlglot
from sqlglot import exp
from .base_detector import BasePatternDetector, DetectionResult
from optimizer_service.models.schemas import ProfiledQuery


class CrossJoinDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        detections = []
        for query in queries:
            try:
                parsed = sqlglot.parse_one(query.sql, read="trino")

                for join_node in parsed.find_all(exp.Join):
                    if join_node.kind and "CROSS" in join_node.kind.upper():
                        message = (
                            f"В запросе (ID: {query.queryid}) обнаружен CROSS JOIN. "
                            f"Это крайне неэффективная операция, которая может приводить к экспоненциальному росту данных. "
                            f"Стратегическая рекомендация: Переписать запрос, добавив явное условие соединения в ON или WHERE, "
                            f"чтобы избежать декартова произведения."
                        )
                        detections.append(DetectionResult(
                            pattern_name="Cross Join",
                            message=message,
                            priority=8,
                            queries=[query]
                        ))
                        break
            except Exception:
                continue
        return detections