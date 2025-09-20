import trino
from urllib.parse import urlparse, parse_qs


class TrinoConnector:
    """
    Класс для управления подключением к Trino.
    Парсит стандартную JDBC строку и предоставляет соединение.
    """

    def __init__(self, jdbc_url: str):
        self._jdbc_url = jdbc_url
        self._parsed_params = self._parse_jdbc_url(jdbc_url)

    def _parse_jdbc_url(self, url: str) -> dict:
        """
        Парсит JDBC URL для извлечения параметров подключения.
        Пример: jdbc:trino://host:port?user=xxx&password=yyy
        """
        if url.startswith("jdbc:"):
            url = url[5:]

        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        user = query_params.get("user", [None])[0]
        password = query_params.get("password", [None])[0]

        path_parts = parsed_url.path.strip('/').split('/')
        catalog = path_parts[0] if path_parts and path_parts[0] else None
        schema = path_parts[1] if len(path_parts) > 1 else None

        return {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 8080,
            "user": user,
            "password": password,
            "catalog": catalog,
            "schema": schema,
        }

    def connect(self) -> trino.dbapi.Connection:
        """
        Создает и возвращает объект соединения с Trino.
        """
        try:
            conn = trino.dbapi.connect(
                host=self._parsed_params["host"],
                port=self._parsed_params["port"],
                user=self._parsed_params["user"],
                http_scheme="https",
                auth=trino.auth.BasicAuthentication(self._parsed_params["user"], self._parsed_params["password"]),
                catalog=self._parsed_params["catalog"],
                schema=self._parsed_params["schema"],
            )
            print(f"Успешное подключение к Trino хосту: {self._parsed_params['host']}")
            return conn
        except Exception as e:
            print(f"Ошибка подключения к Trino: {e}")
            raise