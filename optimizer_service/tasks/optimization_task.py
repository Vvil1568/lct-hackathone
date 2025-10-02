from optimizer_service.llm import llm_provider
from optimizer_service.worker import celery_app
from optimizer_service.models.schemas import TaskRequest
from optimizer_service.data_analyzer.trino_connector import TrinoConnector
from optimizer_service.data_analyzer.analysis_module import AnalysisModule
from optimizer_service.agent.optimization_agent import OptimizationAgent


analyzer_instance = AnalysisModule(connector=None)
agent_instance = OptimizationAgent(llm_provider=llm_provider, analyzer=analyzer_instance)

@celery_app.task(bind=True)
def run_optimization_task(self, task_data: dict):
    print(f"Получена задача {self.request.id}. Полный глобальный цикл.")
    task_request_model = TaskRequest(**task_data)

    try:
        connector = TrinoConnector(jdbc_url=task_request_model.url)
        agent_instance.analyzer._connector = connector

        final_result = agent_instance.run_global_optimization(task_data=task_request_model)

        print(f"Задача {self.request.id} полностью и успешно обработана.")
        return final_result

    except Exception as e:
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА в задаче {self.request.id}: {e}")
        self.update_state(state='FAILURE', meta={'exc': str(e)})
        raise