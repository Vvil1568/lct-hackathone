from optimizer_service.models.schemas import TaskRequest
from optimizer_service.llm.provider import LLMProvider
from optimizer_service.llm.prompts import MEGA_PROMPT_V1_TEMPLATE
import json


class OptimizationAgent:
    """
    Класс-оркестратор, который управляет всем процессом анализа и оптимизации.
    """

    def __init__(self, llm_provider: LLMProvider):
        self.llm_provider = llm_provider

    def _build_prompt(self, task_data: TaskRequest, explain_plan: dict) -> str:
        """
        Собирает все данные вместе и формирует финальный промпт для LLM.
        """

        ddl_context = "\n".join([ddl.statement for ddl in task_data.ddl[:5]])

        query_to_analyze = task_data.queries[0]
        sql_query = query_to_analyze.query
        query_id = query_to_analyze.queryid
        run_quantity = query_to_analyze.runquantity

        explain_plan_str = json.dumps(explain_plan, indent=2)

        prompt = MEGA_PROMPT_V1_TEMPLATE.format(
            ddl_context=ddl_context,
            sql_query=sql_query,
            query_id=query_id,
            run_quantity=run_quantity,
            explain_plan=explain_plan_str
        )
        return prompt

    def run_analysis(self, task_data: TaskRequest, explain_plan: dict) -> dict:
        """
        Основной метод, запускающий пайплайн оптимизации.
        Теперь он вызывает LLM для получения рекомендаций.
        """
        print("Агент начал реальный анализ... Формирую промпт.")

        try:
            prompt = self._build_prompt(task_data, explain_plan)

            print("Промпт сформирован. Отправляю запрос в LLM...")
            llm_response = self.llm_provider.get_completion(prompt)
            print("Агент получил ответ от LLM.")

            return llm_response

        except Exception as e:
            print(f"Критическая ошибка в работе Агента: {e}")
            return {"error": str(e)}