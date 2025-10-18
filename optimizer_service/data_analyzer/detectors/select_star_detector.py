from typing import List, Dict
import sqlglot
from .base_detector import BasePatternDetector
from optimizer_service.models.schemas import ProfiledQuery, DetectionResult


class SelectStarDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        detections = []
        for query in queries:
            try:
                parsed = sqlglot.parse_one(query.sql, read="trino")
                if any(isinstance(sel, sqlglot.exp.Star) for sel in parsed.find(sqlglot.exp.Select).expressions):
                    tables_in_query = [t.sql() for t in parsed.find_all(sqlglot.exp.Table)]
                    for table_name in tables_in_query:
                        if table_name in ddl_map:
                            ddl = ddl_map[table_name]
                            ddl_ast = sqlglot.parse_one(ddl, read="trino")
                            num_columns = len(ddl_ast.this.expressions)
                            if num_columns > 20:
                                message = (
                                    f"The query (ID: {query.queryid}) uses `SELECT *` to read from the wide "
                                    f"table '{table_name}' ({num_columns} columns). This leads to excessive data reading from disk. "
                                    f"Strategic recommendation: Rewrite the query to explicitly list only the necessary columns."
                                )
                                detections.append(DetectionResult(
                                    pattern_name="Select Star on Wide Table",
                                    message=message,
                                    priority=5,
                                    queries=[query],
                                    detector_name=self.__class__.__name__
                                ))
                                break
            except Exception:
                continue
        return detections