import json
import time
import sqlglot
from optimizer_service.models.schemas import TaskRequest, GlobalAnalysisReport
from optimizer_service.llm.provider import LLMProvider
from optimizer_service.llm.prompts import MEGA_PROMPT_V2_TEMPLATE, MEGA_PROMPT_V3_TEMPLATE
from optimizer_service.data_analyzer.analysis_module import AnalysisModule
from optimizer_service.patterns.dispatcher import pattern_dispatcher

MAX_CORRECTION_ATTEMPTS = 2

CORRECTION_PROMPT_TEMPLATE = """
Ты — ведущий дата-архитектор. Твоя предыдущая попытка сгенерировать SQL-код провалила автоматическую валидацию.

# ИСХОДНЫЙ КОНТЕКСТ И ЗАДАЧА
{original_prompt}

# ОШИБКА ВАЛИДАЦИИ
Твой сгенерированный SQL-код был проверен, и вот какая ошибка возникла:
- **Ошибочный SQL:** `{failing_sql}`
- **Сообщение об ошибке:** `{error_message}`

# НОВАЯ ЗАДАЧА
Пожалуйста, исправь эту ошибку, сохранив общую логику оптимизации. 
Перегенерируй ПОЛНЫЙ JSON-ответ в правильном формате.

Твой ответ должен содержать ТОЛЬКО JSON-объект и ничего больше.
"""

class OptimizationAgent:
    def __init__(self, llm_provider: LLMProvider, analyzer: AnalysisModule):
        self.llm_provider = llm_provider
        self.analyzer = analyzer

    def run_global_optimization(self, task_data: TaskRequest) -> dict:
        """
        Запускает полный пайплайн: Глобальный анализ -> Генерация -> Валидация.
        """
        print("Агент: Начинаю глобальную оптимизацию...")

        analysis_report = self.analyzer.perform_global_analysis(task_data)

        if not analysis_report.top_cost_queries:
            return {"error": analysis_report.analysis_summary}

        pattern = pattern_dispatcher.get_pattern(analysis_report.top_detection.detector_name)
        if not pattern:
            raise ValueError(f"Не найден паттерн для детектора {analysis_report.top_detection.detector_name}")

        initial_prompt = self._build_pattern_prompt(analysis_report, task_data, pattern['solution_template'])
        current_prompt = initial_prompt

        for attempt in range(MAX_CORRECTION_ATTEMPTS + 1):
            print(f"--- Попытка генерации #{attempt + 1} ---")
            print(current_prompt)
            if attempt > 0:
                delay = 5 * attempt
                print(f"Делаю паузу в {delay} сек. перед повторной попыткой...")
                time.sleep(delay)

            try:
                llm_response = self.llm_provider.get_completion(current_prompt)
                print(f"LLM Response: {llm_response}")

                is_valid, error_msg, failing_sql = self.analyzer.validate_sql_list(
                    ddl_statements=llm_response.get("ddl", []),
                    migration_statements=llm_response.get("migrations", []),
                    query_statements=llm_response.get("queries", [])
                )

                if is_valid:
                    print("Ответ LLM прошел валидацию!")
                    return llm_response
                else:
                    print("Ответ LLM не прошел валидацию. Готовлю промпт для исправления.")
                    current_prompt = CORRECTION_PROMPT_TEMPLATE.format(
                        original_prompt=initial_prompt,
                        failing_sql=failing_sql,
                        error_message=error_msg
                    )

            except Exception as e:
                print(f"Ошибка на попытке #{attempt + 1}: {e}")
                if attempt >= MAX_CORRECTION_ATTEMPTS:
                    raise e

        raise Exception("Не удалось сгенерировать валидный SQL после нескольких попыток.")

    def _build_pattern_prompt(self, report: GlobalAnalysisReport, task_data: TaskRequest, example: dict) -> str:
        """Собирает отчет и контекст в финальный "стратегический" промпт."""

        top_queries_context_list = []
        for i, q in enumerate(report.top_cost_queries):
            context = (
                f"{i + 1}. Query ID: {q.queryid}\n"
                f"   Cost: {int(q.cost)}\n"
                f"   SQL: {q.sql}"
            )
            top_queries_context_list.append(context)
        top_queries_context = "\n---\n".join(top_queries_context_list)

        relevant_tables = set()
        for q in report.top_cost_queries:
            tables_in_query = q.tables
            print(tables_in_query)
            for table in tables_in_query:
                relevant_tables.add(table)
        print(relevant_tables)
        ddl_context = ""
        try:
            all_ddl_map = {self._extract_table_name_from_ddl(ddl.statement): ddl.statement for ddl in task_data.ddl}
        except Exception:
            all_ddl_map = {}
        print(all_ddl_map)
        for table_name in relevant_tables:
            if table_name in all_ddl_map:
                ddl_context += all_ddl_map[table_name] + "\n"

        highest_cost_query_id = report.top_cost_queries[0].queryid

        example_solution_str = json.dumps(example, indent=2)

        return MEGA_PROMPT_V3_TEMPLATE.format(
            analysis_summary=report.analysis_summary,
            example_solution=example_solution_str,
            top_n=len(report.top_cost_queries),
            top_queries_context=top_queries_context,
            ddl_context=ddl_context,
            highest_cost_query_id=highest_cost_query_id
        )

    def _extract_table_name_from_ddl(self, ddl: str) -> str:
        """Надежная функция для извлечения имени таблицы из CREATE TABLE с помощью AST."""
        try:
            parsed = sqlglot.parse_one(ddl, read="trino")
            return parsed.this.this.sql()
        except Exception:
            return ""