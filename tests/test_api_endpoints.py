import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from optimizer_service.main import app

client = TestClient(app)

IDEAL_RESULT_PAYLOAD = {
    "ddl": [{"statement": "CREATE SCHEMA quests.optimized;"}],
    "migrations": [{"statement": "INSERT INTO quests.optimized.some_table SELECT * FROM quests.public.old_table;"}],
    "queries": [{"queryid": "test-query-id", "query": "SELECT * FROM quests.optimized.some_table;"}]
}

VALID_TASK_REQUEST = {
    "url": "jdbc:trino://fake-host:443",
    "ddl": [{"statement": "CREATE TABLE quests.public.h_author (id int);"}],
    "queries": [{"queryid": "test-query-id", "query": "SELECT * FROM quests.public.h_author", "runquantity": 100,
                 "executiontime": 10}]
}


@pytest.fixture
def mock_celery_task():
    """
    Эта фикстура "мокает" нашу Celery-задачу.
    Вместо реального запуска она возвращает mock-объект, которым мы можем управлять.
    """
    with patch("optimizer_service.api.endpoints.run_optimization_task.delay") as mock_delay:
        mock_async_result = MagicMock()
        mock_async_result.id = "test-task-id-12345"

        mock_delay.return_value = mock_async_result

        yield mock_delay


@pytest.fixture
def mock_async_result():
    """
    Эта фикстура "мокает" объект AsyncResult, который используется для проверки статуса.
    """
    with patch("optimizer_service.api.endpoints.AsyncResult") as mock_async:
        yield mock_async


def test_create_task_endpoint(mock_celery_task):
    """
    Тест 1: Проверяем, что эндпоинт /new работает корректно.
    """
    print("--- Тестируем POST /api/new ---")

    response = client.post("/api/new", json=VALID_TASK_REQUEST)

    assert response.status_code == 202

    mock_celery_task.assert_called_once()

    response_json = response.json()
    assert "taskid" in response_json
    assert response_json["taskid"] == "test-task-id-12345"


def test_get_status_running_endpoint(mock_async_result):
    """
    Тест 2: Проверяем эндпоинт /status для задачи в процессе выполнения.
    """
    print("--- Тестируем GET /api/status (RUNNING) ---")

    mock_instance = mock_async_result.return_value
    mock_instance.state = "PENDING"

    response = client.get("/api/status", params={"task_id": "some-task-id"})

    assert response.status_code == 200
    response_json = response.json()
    assert response_json["status"] == "RUNNING"


def test_get_status_done_endpoint(mock_async_result):
    """
    Тест 3: Проверяем эндпоинт /status для завершенной задачи.
    """
    print("--- Тестируем GET /api/status (DONE) ---")

    mock_instance = mock_async_result.return_value
    mock_instance.state = "SUCCESS"

    response = client.get("/api/status", params={"task_id": "some-task-id"})

    assert response.status_code == 200
    response_json = response.json()
    assert response_json[
               "status"] == "DONE"


def test_get_result_endpoint_success(mock_async_result):
    """
    Тест 4: ГЛАВНЫЙ ТЕСТ. Проверяем, что /getresult возвращает корректную схему.
    """
    print("--- Тестируем GET /api/getresult (SUCCESS) ---")

    mock_instance = mock_async_result.return_value
    mock_instance.ready.return_value = True
    mock_instance.successful.return_value = True
    mock_instance.get.return_value = IDEAL_RESULT_PAYLOAD

    response = client.get("/api/getresult", params={"task_id": "some-task-id"})

    assert response.status_code == 200

    assert response.json() == IDEAL_RESULT_PAYLOAD


def test_get_result_endpoint_not_ready(mock_async_result):
    """
    Тест 5: Проверяем, что /getresult возвращает ошибку, если задача не готова.
    """
    print("--- Тестируем GET /api/getresult (NOT READY) ---")

    mock_instance = mock_async_result.return_value
    mock_instance.ready.return_value = False

    response = client.get("/api/getresult", params={"task_id": "some-task-id"})

    assert response.status_code == 200
    assert "error" in response.json()