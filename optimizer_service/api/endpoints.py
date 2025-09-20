from fastapi import APIRouter
from celery.result import AsyncResult
from optimizer_service.models.schemas import TaskRequest, TaskResponse, TaskStatus, OptimizationResult
from optimizer_service.tasks.optimization_task import run_optimization_task

router = APIRouter()

@router.post("/new", response_model=TaskResponse, status_code=202)
def create_task(request: TaskRequest):
    """
    Запускает новую задачу оптимизации.
    """
    task = run_optimization_task.delay(request.dict())
    return TaskResponse(taskid=task.id)

@router.get("/status/{task_id}", response_model=TaskStatus)
def get_task_status(task_id: str):
    """
    Возвращает статус задачи (RUNNING, DONE, FAILED).
    """
    task_result = AsyncResult(task_id, app=run_optimization_task.app)
    status = "RUNNING" if task_result.state == "PENDING" or task_result.state == "STARTED" else task_result.state
    return TaskStatus(status=status)

@router.get("/getresult/{task_id}", response_model=OptimizationResult)
def get_task_result(task_id: str):
    """
    Возвращает результат выполненной задачи.
    """
    task_result = AsyncResult(task_id, app=run_optimization_task.app)
    if task_result.ready():
        if task_result.successful():
            return OptimizationResult(**task_result.get())
        else:
            return {"error": "Task failed"}
    else:
        return {"error": "Task is not ready yet"}