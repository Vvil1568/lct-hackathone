from typing import List, Dict
import sqlglot.expressions as exp
import sqlglot
from .base_detector import BasePatternDetector
from optimizer_service.models.schemas import ProfiledQuery, DetectionResult


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
                            f"Inefficient filtering in the HAVING clause was detected in query (ID: {query.queryid}). "
                            f"The condition '{condition.sql()}' does not use an aggregate function and can be moved to the WHERE clause for a significant "
                            f"performance boost, as filtering will occur before the expensive grouping operation. "
                            f"Strategic recommendation: Rewrite the query by moving non-aggregate conditions from HAVING to WHERE."
                        )
                        detections.append(DetectionResult(
                            pattern_name="Inefficient HAVING Clause",
                            message=message,
                            priority=7,
                            queries=[query],
                            detector_name=self.__class__.__name__,
                        ))
                        break
            except Exception:
                continue
        return detections