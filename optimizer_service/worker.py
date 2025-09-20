from celery import Celery
from optimizer_service.core.config import settings

celery_app = Celery(
    "tasks",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["optimizer_service.tasks.optimization_task"]
)

celery_app.conf.update(
    task_track_started=True,
)