import json
import re
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

    def validate_sql_list(self, ddl_statements: list, migration_statements: list, query_statements: list) -> (
    bool, str, str):
        """
        Проверяет SQL-запросы с помощью симуляции через CTE. Не требует прав на запись.
        """
        print("Запускаю CTE-валидацию сгенерированного SQL...")

        try:
            if not ddl_statements or not migration_statements or not query_statements:
                print("Предупреждение: Недостаточно данных для CTE-валидации. Пропускаю.")
                return (True, "", "")

            create_table_sql = next(
                (s["statement"] for s in ddl_statements if "CREATE TABLE" in s["statement"].upper()), None)
            if not create_table_sql:
                return (False, "Не найден CREATE TABLE в DDL ответа LLM", "")

            try:
                columns_part = re.search(r'\((.*)\) WITH', create_table_sql, re.DOTALL | re.IGNORECASE).group(1)
                column_names = [line.strip().split()[0] for line in columns_part.strip().split(',')]
            except Exception:
                return (False, "Не удалось распарсить колонки из CREATE TABLE", create_table_sql)

            match = re.search(r"CREATE TABLE\s+([^\s\(]+)", create_table_sql, re.IGNORECASE)
            if not match:
                return (False, "Не удалось извлечь имя новой таблицы из DDL", create_table_sql)
            new_table_name = match.group(1)

            migration_sql = migration_statements[0]["statement"]
            select_part_index = migration_sql.upper().find("SELECT")
            if select_part_index == -1:
                return (False, "Не найдена SELECT-часть в миграционном скрипте", migration_sql)
            migration_select = migration_sql[select_part_index:]

            if migration_select.strip().endswith(';'):
                migration_select = migration_select.strip()[:-1]

            final_query_sql = query_statements[0]["query"]
            final_query_simulated = final_query_sql.replace(new_table_name, "simulated_new_table")

            validation_query = f"""
            WITH simulated_new_table ({', '.join(column_names)}) AS (
                {migration_select}
            )
            {final_query_simulated}
            """

            if validation_query.strip().endswith(';'):
                validation_query = validation_query.strip()[:-1]

            explain_query = f"EXPLAIN {validation_query}"

            print(f"Выполняю EXPLAIN для симуляции...")
            with self._connector.connect() as conn:
                cur = conn.cursor()
                cur.execute(explain_query)

            print("CTE-валидация SQL прошла успешно.")
            return (True, "", "")

        except Exception as e:
            error_message = f"Критическая ошибка в процессе CTE-валидации: {e}"
            print(error_message)
            return (False, error_message, validation_query if 'validation_query' in locals() else "")
