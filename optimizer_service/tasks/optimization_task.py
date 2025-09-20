from optimizer_service.worker import celery_app
from optimizer_service.agent.optimization_agent import optimization_agent
from optimizer_service.models.schemas import TaskRequest

@celery_app.task(bind=True)
def run_optimization_task(self, task_data: dict):
    """
    Эта задача теперь делегирует всю работу нашему Агенту.
    """
    print(f"Получена задача {self.request.id}. Передаю агенту...")
    
    task_request_model = TaskRequest(**task_data)

    result = optimization_agent.run_analysis(task_request_model)
    
    print(f"Задача {self.request.id} обработана агентом.")
    
    return result