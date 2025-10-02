import json
from typing import List

import sqlglot
from sqlglot import exp

from optimizer_service.data_analyzer.trino_connector import TrinoConnector
from optimizer_service.models.schemas import ProfiledQuery, GlobalAnalysisReport, TaskRequest, DetectionResult
from .detectors.cross_join_detector import CrossJoinDetector
from .detectors.inefficient_agg_detector import InefficientAggregationDetector
from .detectors.join_detector import JoinPatternDetector
from .detectors.partitioning_detector import PartitioningCandidateDetector
from .detectors.select_star_detector import SelectStarDetector


class AnalysisModule:
    def __init__(self, connector: TrinoConnector):
        self._connector = connector
        self._detectors = [
            JoinPatternDetector(),
            PartitioningCandidateDetector(),
            CrossJoinDetector(),
            InefficientAggregationDetector(),
            SelectStarDetector(),
        ]

    def perform_global_analysis(self, task_data: TaskRequest) -> GlobalAnalysisReport:
        print("Начинаю глобальный анализ с использованием детекторов...")

        profiled_queries = self._profile_all_queries(task_data)
        top_cost_queries = self._prioritize_queries(profiled_queries)

        ddl_map = {sqlglot.parse_one(ddl.statement).this.this.sql(): ddl.statement for ddl in task_data.ddl}

        all_detections: List[DetectionResult] = []
        for detector in self._detectors:
            all_detections.extend(detector.run(top_cost_queries, ddl_map))

        sorted_detections = sorted(all_detections, key=lambda d: d.priority, reverse=True)

        highest_priority_problem = sorted_detections[0] if sorted_detections else None
        analysis_summary = self._generate_analysis_summary(sorted_detections, top_cost_queries)

        return GlobalAnalysisReport(
            top_cost_queries=top_cost_queries,
            analysis_summary=analysis_summary,
            top_detection=highest_priority_problem
        )

    def _generate_analysis_summary(self, detections: List[DetectionResult], top_queries) -> str:
        if not top_queries:
            return "Не удалось проанализировать запросы."

        if not detections:
            return ("Глобальный анализ не выявил очевидных паттернов. "
                    "Рекомендуется поочередная оптимизация самого дорогого запроса.")

        highest_priority_problem = detections[0]
        return highest_priority_problem.message

    def _profile_all_queries(self, task_data: TaskRequest) -> List[ProfiledQuery]:
        """Собирает EXPLAIN и метаданные для каждого запроса."""
        results = []
        with self._connector.connect() as conn:
            cur = conn.cursor()
            for query in task_data.queries:
                try:
                    print(f"Профилирую запрос: {query.queryid}")
                    explain_plan = self._run_explain_with_cursor(cur, query.query)

                    parsed_sql = sqlglot.parse_one(query.query, read="trino")
                    tables = []
                    for table in parsed_sql.find_all(exp.Table):
                        table.set('alias', None)
                        tables.append(table.sql())
                    print(tables)
                    results.append(
                        ProfiledQuery(
                            queryid=query.queryid,
                            sql=query.query,
                            run_quantity=query.runquantity,
                            execution_time=query.executiontime,
                            explain_plan=explain_plan,
                            tables=tables
                        )
                    )
                except Exception as e:
                    print(f"Не удалось спрофилировать запрос {query.queryid}. Ошибка: {e}. Пропускаю.")
        return results

    def _prioritize_queries(self, queries: List[ProfiledQuery], top_n: int = 5) -> List[ProfiledQuery]:
        """Вычисляет 'стоимость' и возвращает самые дорогие запросы."""
        for query in queries:
            query.cost = query.run_quantity * query.execution_time

        sorted_queries = sorted(queries, key=lambda q: q.cost, reverse=True)
        return sorted_queries[:top_n]

    def _run_explain_with_cursor(self, cursor, sql_query: str) -> dict:
        """Выполняет EXPLAIN, используя существующий курсор. Максимально отказоустойчивая версия."""
        if sql_query.strip().endswith(';'):
            sql_query = sql_query.strip()[:-1]

        try:
            explain_query = f"EXPLAIN (FORMAT JSON) {sql_query}"
            cursor.execute(explain_query)
            result = cursor.fetchone()
        except Exception as e:
            raise ValueError(f"Выполнение EXPLAIN провалилось с ошибкой БД: {e}")

        if result and isinstance(result, (list, tuple)) and len(result) > 0 and isinstance(result[0], str):
            try:
                return json.loads(result[0])
            except json.JSONDecodeError:
                raise ValueError(f"EXPLAIN вернул строку, но это невалидный JSON: {result[0][:200]}...")
        else:
            raise ValueError(f"EXPLAIN не вернул ожидаемую JSON-строку. Получено (тип: {type(result)}): {result}")

    def validate_sql_list(self, ddl_statements: list, migration_statements: list, query_statements: list) -> (
    bool, str, str):
        """
        Проверяет SQL-запросы с помощью симуляции через CTE. Не требует прав на запись.
        """
        print("Запускаю CTE-валидацию сгенерированного SQL...")

        try:
            if not ddl_statements or not migration_statements or not query_statements:
                print("Предупреждение: Недостаточно данных для CTE-валидации. Пропускаю.")
                return True, "", ""

            create_table_sql = next(
                (s["statement"] for s in ddl_statements if "CREATE TABLE" in s["statement"].upper()), None)
            if not create_table_sql:
                return False, "Не найден CREATE TABLE в DDL ответа LLM", ""

            try:
                create_table_ast = sqlglot.parse_one(create_table_sql, read="trino")
                column_defs = create_table_ast.this.expressions
                column_names = [col_def.this.sql() for col_def in column_defs]
            except Exception as e:
                return False, f"AST-парсер не смог извлечь колонки из DDL: {e}", create_table_sql

            new_table_name = sqlglot.parse_one(create_table_sql, read="trino").this.this.sql()
            if not new_table_name:
                return False, "Не удалось извлечь имя новой таблицы из DDL", create_table_sql

            migration_sql = migration_statements[0]["statement"]
            select_part_index = migration_sql.upper().find("SELECT")
            if select_part_index == -1:
                return False, "Не найдена SELECT-часть в миграционном скрипте", migration_sql
            migration_select = migration_sql[select_part_index:]

            if migration_select.strip().endswith(';'):
                migration_select = migration_select.strip()[:-1]

            final_query_sql = query_statements[0]["query"]
            final_query_simulated = final_query_sql.replace(new_table_name, "simulated_new_table")

            validation_query = f"""
            WITH simulated_new_table ({', '.join(column_names)}) AS (
                {migration_select}
            )
            {final_query_simulated}
            """

            if validation_query.strip().endswith(';'):
                validation_query = validation_query.strip()[:-1]

            explain_query = f"EXPLAIN {validation_query}"

            print(f"Выполняю EXPLAIN для симуляции...")
            print(f"EXPLAIN: {explain_query}")
            with self._connector.connect() as conn:
                cur = conn.cursor()
                cur.execute(explain_query)

            print("CTE-валидация SQL прошла успешно.")
            return True, "", ""

        except Exception as e:
            error_message = f"Критическая ошибка в процессе CTE-валидации: {e}"
            print(error_message)
            return False, error_message, validation_query if 'validation_query' in locals() else ""
