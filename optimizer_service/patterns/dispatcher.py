import json
from pathlib import Path

class PatternDispatcher:
    def __init__(self, library_path: Path):
        try:
            with open(library_path, 'r', encoding='utf-8') as f:
                self._library = json.load(f)
            print(f"Библиотека паттернов успешно загружена из {library_path}")
        except Exception as e:
            print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить библиотеку паттернов: {e}")
            self._library = {}

    def get_pattern(self, detector_name: str) -> dict:
        """
        Возвращает шаблон решения для заданного имени детектора.
        """
        return self._library.get(detector_name)

patterns_library_path = Path(__file__).parent / "library.json"
pattern_dispatcher = PatternDispatcher(library_path=patterns_library_path)