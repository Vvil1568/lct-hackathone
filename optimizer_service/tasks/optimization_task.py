from optimizer_service.worker import celery_app
from optimizer_service.models.schemas import TaskRequest
from optimizer_service.data_analyzer.trino_connector import TrinoConnector
from optimizer_service.data_analyzer.analysis_module import AnalysisModule

@celery_app.task(bind=True)
def run_optimization_task(self, task_data: dict):
    """
    Эта задача теперь подключается к Trino и выполняет базовый анализ.
    """
    print(f"Получена задача {self.request.id}. Начинаю реальную обработку...")
    task_request_model = TaskRequest(**task_data)

    try:
        connector = TrinoConnector(jdbc_url=task_request_model.url)
        analyzer = AnalysisModule(connector=connector)

        if task_request_model.queries:
            first_query = task_request_model.queries[0]
            print("\n--- Анализ SQL запроса ---")
            explain_plan = analyzer.run_explain(first_query.query)
            print(f"Получен EXPLAIN план для queryid {first_query.queryid}.")
            print(str(explain_plan)[:500] + "...")

        if task_request_model.ddl:

            ddl_statement = task_request_model.ddl[0].statement
            table_name = ddl_statement.split("TABLE")[1].strip().split("(")[0].strip()
            print(f"\n--- Анализ DDL таблицы {table_name} ---")
            table_stats = analyzer.get_table_stats(table_name)
            print(f"Получена статистика для таблицы {table_name}:")
            print(table_stats)

        print(f"\nЗадача {self.request.id} успешно проанализирована.")

    except Exception as e:
        print(f"\n!!! КРИТИЧЕСКАЯ ОШИБКА в задаче {self.request.id}: {e}")
        raise e

    dummy_result = {
        "ddl": [{"statement": "CREATE SCHEMA data_optimized_dummy;"}],
        "migrations": [{"statement": "INSERT INTO dummy ... SELECT ...;"}],
        "queries": [{"queryid": task_request_model.queries[0].queryid if task_request_model.queries else "dummy-id",
                     "query": "SELECT * FROM data_optimized_dummy.new_table;"}]
    }

    return dummy_result