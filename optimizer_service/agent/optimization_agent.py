from optimizer_service.models.schemas import TaskRequest
from optimizer_service.llm.provider import LLMProvider
from optimizer_service.llm.prompts import MEGA_PROMPT_V1_TEMPLATE
from optimizer_service.data_analyzer.analysis_module import AnalysisModule
import json

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

    def run_analysis_and_generation(self, task_data: TaskRequest, explain_plan: dict) -> dict:
        print("Агент начал полный цикл: Генерация -> Валидация -> Исправление.")

        initial_prompt = self._build_prompt(task_data, explain_plan)
        current_prompt = initial_prompt

        for attempt in range(MAX_CORRECTION_ATTEMPTS + 1):
            print(f"--- Попытка генерации #{attempt + 1} ---")

            try:
                llm_response = self.llm_provider.get_completion(current_prompt)
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

    def _build_prompt(self, task_data: TaskRequest, explain_plan: dict) -> str:
        ddl_context = "\n".join([ddl.statement for ddl in task_data.ddl])
        if not task_data.queries:
            raise ValueError("Список запросов (queries) пуст.")

        query_to_analyze = task_data.queries[0]
        explain_plan_str = json.dumps(explain_plan, indent=2)

        return MEGA_PROMPT_V1_TEMPLATE.format(
            ddl_context=ddl_context,
            sql_query=query_to_analyze.query,
            query_id=query_to_analyze.queryid,
            run_quantity=query_to_analyze.runquantity,
            explain_plan=explain_plan_str
        )