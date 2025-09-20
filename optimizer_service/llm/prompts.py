MEGA_PROMPT_V1_TEMPLATE = """
Ты — ведущий дата-архитектор, специализирующийся на оптимизации Data Lakehouse на Trino и Iceberg. 
Твоя задача — проанализировать предоставленный DDL, SQL-запрос и его EXPLAIN-план, чтобы предложить 
оптимизацию.

# КОНТЕКСТ
Вот DDL задействованных таблиц:
```sql
{ddl_context}
```

Вот SQL-запрос для анализа:
- Query ID: {query_id}
- Частота запусков: {run_quantity}
```sql
{sql_query}
```

Вот его план выполнения (EXPLAIN), который показывает узкие места:
```json
{explain_plan}
```

# ЗАДАЧА
Основываясь на анализе, предложи новую, оптимизированную структуру данных и перепиши под нее запрос.
Ты должен сгенерировать:
1. DDL для создания новых таблиц в новой схеме 'quests.optimized'.
2. SQL-скрипты для миграции данных из старых таблиц в новые.
3. Переписанный SQL-запрос, который использует новые таблицы.

# ВАЖНЫЕ ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ
1. Используй СТРОГО синтаксис Trino SQL.
2. Для партиционирования таблиц Iceberg используй конструкцию `WITH (partitioning = ARRAY['column_name'])`.
3. Не используй синтаксис `PARTITION OF`.
4. Всегда указывай полный путь к таблицам: `<каталог>.<схема>.<таблица>`.

# ФОРМАТ ОТВЕТА
Верни ответ СТРОГО в формате JSON. Вот пример идеальной структуры ответа:
```json
{{
  "ddl": [
    {{"statement": "CREATE SCHEMA IF NOT EXISTS quests.optimized;"}},
    {{"statement": "CREATE TABLE quests.optimized.new_table (...) WITH (partitioning = ARRAY['day(event_dt)']);"}}
  ],
  "migrations": [
    {{"statement": "INSERT INTO quests.optimized.new_table SELECT ... FROM quests.public.old_table ...;"}}
  ],
  "queries": [
    {{"queryid": "{query_id}", "query": "SELECT ... FROM quests.optimized.new_table;"}}
  ]
}}
```

Теперь, сгенерируй решение для предоставленного контекста.
"""
