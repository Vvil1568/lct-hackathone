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
            return {"error": "Task failed"}
    else:
        return {"error": "Task is not ready yet"}