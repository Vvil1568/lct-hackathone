import json
from optimizer_service.data_analyzer.trino_connector import TrinoConnector


class AnalysisModule:
    """
    Модуль для выполнения аналитических запросов к Trino.
    """

    def __init__(self, connector: TrinoConnector):
        self._connector = connector

    def run_explain(self, sql_query: str) -> dict:
        """
        Выполняет 'EXPLAIN (FORMAT JSON)' для заданного SQL запроса.
        """
        print(f"Выполняю EXPLAIN для запроса: {sql_query[:100]}...")
        if sql_query.strip().endswith(';'):
            sql_query = sql_query.strip()[:-1]

        explain_query = f"EXPLAIN (FORMAT JSON) {sql_query}"

        try:
            with self._connector.connect() as conn:
                cur = conn.cursor()
                cur.execute(explain_query)
                result = cur.fetchone()

                if result and result[0]:
                    return json.loads(result[0])
                else:
                    raise ValueError("EXPLAIN не вернул результат.")
        except Exception as e:
            print(f"Ошибка при выполнении EXPLAIN: {e}")
            raise

    def get_table_stats(self, table_name: str) -> list:
        """
        Выполняет 'SHOW STATS FOR table' и возвращает статистику.
        """
        print(f"Получаю статистику для таблицы: {table_name}...")
        stats_query = f"SHOW STATS FOR {table_name}"

        try:
            with self._connector.connect() as conn:
                cur = conn.cursor()
                cur.execute(stats_query)
                stats = cur.fetchall()
                return stats
        except Exception as e:
            print(f"Ошибка при получении статистики для таблицы {table_name}: {e}")
            raise