from optimizer_service.worker import celery_app
from optimizer_service.models.schemas import TaskRequest
from optimizer_service.data_analyzer.trino_connector import TrinoConnector
from optimizer_service.data_analyzer.analysis_module import AnalysisModule
from optimizer_service.agent.optimization_agent import OptimizationAgent
from optimizer_service.llm.provider import llm_provider

optimization_agent = OptimizationAgent(llm_provider=llm_provider)


# ----------------------------------------------------------------

@celery_app.task(bind=True)
def run_optimization_task(self, task_data: dict):
    """
    Полный пайплайн: Анализ данных -> Вызов Агента с LLM
    """
    print(f"Получена задача {self.request.id}. Полный цикл обработки.")
    task_request_model = TaskRequest(**task_data)

    try:
        connector = TrinoConnector(jdbc_url=task_request_model.url)
        analyzer = AnalysisModule(connector=connector)

        first_query = task_request_model.queries[0]
        print(f"Собираю EXPLAIN для queryid {first_query.queryid}...")
        explain_plan = analyzer.run_explain(first_query.query)
        print("EXPLAIN получен.")

        result = optimization_agent.run_analysis(
            task_data=task_request_model,
            explain_plan=explain_plan
        )

        print(f"Задача {self.request.id} полностью обработана.")
        return result

    except Exception as e:
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в задаче {self.request.id}: {e}")
        self.update_state(state='FAILURE', meta={'exc': str(e)})
        raise