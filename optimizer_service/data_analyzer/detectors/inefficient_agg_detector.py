from typing import List, Dict
import sqlglot.expressions as exp
import sqlglot
from .base_detector import BasePatternDetector, DetectionResult
from optimizer_service.models.schemas import ProfiledQuery

class InefficientAggregationDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        detections = []
        for query in queries:
            try:
                parsed = sqlglot.parse_one(query.sql, read="trino")
                having_clause = parsed.find(exp.Having)

                if not having_clause:
                    continue

                for condition in having_clause.this.find_all(exp.Binary):
                    is_comparison = isinstance(condition, (
                        exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ
                    ))

                    if not is_comparison:
                        continue

                    if not condition.left.find(exp.AggFunc) and not condition.right.find(exp.AggFunc):
                        message = (
                            f"В запросе (ID: {query.queryid}) обнаружена неэффективная фильтрация в HAVING. "
                            f"Условие '{condition.sql()}' не использует агрегацию и может быть перенесено в WHERE для значительного "
                            f"ускорения, так как фильтрация произойдет до дорогостоящей группировки. "
                            f"Стратегическая рекомендация: Переписать запрос, переместив неагрегатные условия из HAVING в WHERE."
                        )
                        detections.append(DetectionResult(
                            pattern_name="Inefficient HAVING Clause",
                            message=message,
                            priority=7,
                            queries=[query]
                        ))
                        break
            except Exception:
                continue
        return detections