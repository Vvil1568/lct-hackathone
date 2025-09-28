from collections import Counter
from typing import List, Dict
import sqlglot
from .base_detector import BasePatternDetector, DetectionResult
from optimizer_service.models.schemas import ProfiledQuery


class PartitioningCandidateDetector(BasePatternDetector):
    def run(self, queries: List[ProfiledQuery], ddl_map: Dict[str, str]) -> List[DetectionResult]:
        if not queries:
            return []

        table_counter = Counter()
        for query in queries:
            table_counter.update(query.tables)

        if not table_counter:
            return []

        candidate_table, usage_count = table_counter.most_common(1)[0]

        if usage_count < 2 or candidate_table not in ddl_map:
            return []

        candidate_ddl = ddl_map[candidate_table]

        if "partitioning" in candidate_ddl.lower():
            return []

        filter_columns = Counter()
        for query in queries:
            if candidate_table in query.tables:
                try:
                    parsed = sqlglot.parse_one(query.sql, read="trino")
                    if parsed.find(sqlglot.exp.Where):
                        for column in parsed.find(sqlglot.exp.Where).find_all(sqlglot.exp.Column):
                            if column.this:
                                filter_columns.update([column.this.sql()])
                except Exception:
                    continue

        if not filter_columns:
            return []

        top_keys = [col[0] for col in filter_columns.most_common(2)]
        keys_str = " и ".join([f"'{k}'" for k in top_keys])

        message = (
            f"Обнаружен паттерн 'Непартиционированная Широкая Таблица'. "
            f"Таблица '{candidate_table}' часто используется в самых дорогих запросах, но не партиционирована. "
            f"Анализ запросов показывает, что данные часто фильтруются по колонкам {keys_str}. "
            f"Стратегическая рекомендация: Создать партиционированную копию таблицы '{candidate_table}', "
            f"используя эти колонки в качестве ключей (например, WITH (partitioning = ARRAY[{', '.join(top_keys)}]))."
        )

        return [DetectionResult(
            pattern_name="Partitioning Candidate",
            message=message,
            priority=9,
            queries=queries
        )]