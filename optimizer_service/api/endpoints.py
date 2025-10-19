from typing import Union

import redis
from fastapi import APIRouter
from celery.result import AsyncResult

from optimizer_service.core.config import settings
from optimizer_service.models.schemas import TaskRequest, TaskResponse, TaskStatus, OptimizationResult, ErrorResponse, \
    TaskLogsResponse
from optimizer_service.tasks.optimization_task import run_optimization_task

router = APIRouter()

@router.post("/new", response_model=TaskResponse, status_code=202)
def create_task(request: TaskRequest):
    """
    Запускает новую задачу оптимизации.
    """
    task = run_optimization_task.delay(request.dict())
    return TaskResponse(taskid=task.id)

@router.get("/status", response_model=TaskStatus)
def get_task_status(task_id: str):
    """
    Возвращает статус задачи (RUNNING, DONE, FAILED).
    """
    task_result = AsyncResult(task_id, app=run_optimization_task.app)
    celery_status = task_result.state

    if celery_status in ["PENDING", "STARTED", "RETRY"]:
        status = "RUNNING"
    elif celery_status == "SUCCESS":
        status = "DONE"
    else:
        status = "FAILED"
    return TaskStatus(status=status)

@router.get("/getresult", response_model=Union[OptimizationResult, ErrorResponse])
def get_task_result(task_id: str):
    """
    Возвращает результат выполненной задачи.
    """
    task_result = AsyncResult(task_id, app=run_optimization_task.app)
    if task_result.ready():
        if task_result.successful():
            return OptimizationResult(**task_result.get())
        else:
            error_info = str(task_result.info) if task_result.info else "Unknown error"
            return ErrorResponse(error=f"Task failed: {error_info}")
    else:
        return ErrorResponse(error="Task is not ready yet")


@router.get("/logs", response_model=TaskLogsResponse)
def get_task_logs(task_id: str):
    """
    Возвращает полный лог выполнения для указанной задачи.
    """
    try:
        redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        log_key = f"task_logs:{task_id}"

        logs = redis_client.lrange(log_key, 0, -1)

        if not logs:
            return TaskLogsResponse(task_id=task_id, logs=["Логи для данной задачи не найдены."])

        return TaskLogsResponse(task_id=task_id, logs=logs)
    except Exception as e:
        return TaskLogsResponse(task_id=task_id, logs=[f"Ошибка при чтении логов: {e}"])