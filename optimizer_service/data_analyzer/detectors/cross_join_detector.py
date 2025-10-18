from typing import List, Dict
import sqlglot
from sqlglot import exp
from .base_detector import BasePatternDetector
from optimizer_service.models.schemas import ProfiledQuery, DetectionResult


class CrossJoinDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        detections = []
        for query in queries:
            try:
                parsed = sqlglot.parse_one(query.sql, read="trino")

                for join_node in parsed.find_all(exp.Join):
                    if join_node.kind and "CROSS" in join_node.kind.upper():
                        message = (
                            f"A CROSS JOIN was detected in query (ID: {query.queryid}). "
                            f"This is a highly inefficient operation that can lead to an exponential increase in data. "
                            f"Strategic recommendation: Rewrite the query by adding an explicit join condition in the ON or WHERE clause to avoid a Cartesian product."
                        )
                        detections.append(DetectionResult(
                            pattern_name="Cross Join",
                            message=message,
                            priority=8,
                            queries=[query],
                            detector_name=self.__class__.__name__,
                        ))
                        break
            except Exception:
                continue
        return detections