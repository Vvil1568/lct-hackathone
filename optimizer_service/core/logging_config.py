import logging
import redis
from celery.signals import worker_process_init

from optimizer_service.core.config import settings
from optimizer_service.core.context import current_task_id


class TaskIdFilter(logging.Filter):
    def filter(self, record):
        record.task_id = current_task_id.get()
        return True


class RedisHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

    def emit(self, record):
        if hasattr(record, 'task_id') and record.task_id:
            log_entry = self.format(record)
            log_key = f"task_logs:{record.task_id}"
            try:
                self.redis_client.rpush(log_key, log_entry)
                self.redis_client.expire(log_key, 3600)
            except Exception as e:
                print(f"Failed to log to Redis: {e}")
                print(log_entry)


@worker_process_init.connect(weak=False)
def setup_celery_logging(**kwargs):
    """
    Эта функция будет автоматически вызвана Celery в каждом новом процессе-воркере.
    """
    print("--- Настройка логгера для процесса-воркера Celery ---")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

    redis_handler = RedisHandler()
    redis_handler.setFormatter(formatter)

    task_id_filter = TaskIdFilter()
    redis_handler.addFilter(task_id_filter)

    logger.addHandler(redis_handler)



    print("--- Логгер для Redis успешно настроен ---")