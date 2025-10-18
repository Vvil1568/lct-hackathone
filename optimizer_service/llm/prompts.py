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
Твой ответ должен содержать ТОЛЬКО JSON-объект и ничего больше. Не добавляй никаких объяснений или форматирования ```json.
"""


MEGA_PROMPT_V2_TEMPLATE = """
Ты — ведущий дата-архитектор Trino. Проведен автоматический анализ производительности, 
и твоя задача — сгенерировать решение на основе его выводов.

# РЕЗУЛЬТАТЫ АВТОМАТИЧЕСКОГО АНАЛИЗА
{analysis_summary}

# КОНТЕКСТ: САМЫЕ "ДОРОГИЕ" ЗАПРОСЫ
Вот топ-{top_n} запросов, которые создают основную нагрузку, отсортированные по важности:
---
{top_queries_context}
---

# КОНТЕКСТ: DDL ЗАДЕЙСТВОВАННЫХ ТАБЛИЦ
Вот DDL для таблиц, которые чаще всего встречаются в проблемных запросах:```sql
{ddl_context}
```

# ЗАДАЧА
Основываясь на стратегической рекомендации из анализа, предложи новую, оптимизированную 
структуру данных. Ты должен сгенерировать полный пайплайн:
1. DDL для создания новых таблиц в новой схеме 'quests.optimized'.
2. SQL-скрипты для миграции данных из старых таблиц в новые.
3. Переписанный SQL для **самого дорогого запроса** (ID: {highest_cost_query_id}).

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
    {{"queryid": "{highest_cost_query_id}", "query": "SELECT ... FROM quests.optimized.new_table;"}}
  ]
}}
```

Теперь, сгенерируй решение. Твой ответ должен содержать ТОЛЬКО JSON-объект и ничего больше.
"""



MEGA_PROMPT_V3_TEMPLATE = """
Ты — ведущий дата-архитектор Trino. Твоя задача — адаптировать готовый пример решения под текущий контекст.

# РЕЗУЛЬТАТЫ АНАЛИЗА И СТРАТЕГИЯ
{analysis_summary}

# ГОТОВЫЙ ПРИМЕР РЕШЕНИЯ ИЗ БАЗЫ ЗНАНИЙ
Вот идеальный пример решения для данного типа проблемы. Используй его как основной шаблон для структуры и формата ответа:
```json
{example_solution}
```

# ТЕКУЩИЙ КОНТЕКСТ
Вот топ-{top_n} самых "дорогих" запросов, которые вызвали эту проблему:
---
{top_queries_context}
---
Вот DDL таблиц, задействованных в этих запросах:
```sql
{ddl_context}```

# ЗАДАЧА
Адаптируй "ГОТОВЫЙ ПРИМЕР РЕШЕНИЯ" для "ТЕКУЩЕГО КОНТЕКСТА". 
- Замени плейсхолдеры, такие как `table1`, `col_a`, на реальные имена таблиц и колонок из проблемных запросов.
- Убедись, что `queryid` в ответе соответствует ID самого дорогого запроса: `{highest_cost_query_id}`.
- Сохраняй всю специфику Trino/Iceberg из примера (`WITH (partitioning = ...)`).

Верни ТОЛЬКО финальный JSON-объект без каких-либо объяснений.
"""

MEGA_PROMPT_V4_TEMPLATE = """
You are a world-class Trino data architect. An automated performance analysis has been performed,
and your task is to generate a solution based on its findings.

# AUTOMATED ANALYSIS RESULTS & STRATEGY
{analysis_summary}

# GOLDEN EXAMPLE SOLUTION FROM KNOWLEDGE BASE
Here is an ideal solution example for this type of problem. Use it as the primary template for the response structure and format:
```json
{example_solution}
```

# CURRENT CONTEXT
Here are the top-{top_n} most "expensive" queries that caused this problem, sorted by importance:
---
{top_queries_context}
---
Here are the DDLs for the tables involved in these queries:
```sql
{ddl_context}
```

# YOUR TASK
Adapt the "GOLDEN EXAMPLE SOLUTION" to the "CURRENT CONTEXT".
- Replace placeholders like `table1`, `col_a` with actual table and column names from the problematic queries.
- Ensure the `queryid` in your response matches the ID of the most expensive query: `{highest_cost_query_id}`.
- Preserve all Trino/Iceberg specifics from the example (`WITH (partitioning = ...)`).

Return ONLY the final JSON object without any explanations or markdown formatting.
"""