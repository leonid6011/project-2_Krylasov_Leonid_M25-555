# src/primitive_db/utils.py

"""
Вспомогательные функции для работы с файлами метаданных баз данных.
"""

import json
from typing import Any, Dict, List

from .constants import DATA_DIR, METADATA_FILE


def load_metadata() -> Dict[str, Dict[str, str]]:
    """
    Загружаем метаданные базы данных из файла *.json.
    Если файла нет или он поврежден вернется пустой словарь.
    """
    if not METADATA_FILE.exists():
        return {}
    
    try:
        with METADATA_FILE.open("r", encoding="utf-8") as f:
            data: Any = json.load(f)
    except (json.JSONDecodeError, OSError):
        #если файл битый или не читается - считаемб что БД пуста.
        return {}
    
    #Гарантируем, что вернем ровно dict[str, dict[str, str]]
    if not isinstance(data, dict):
        return {}
    
    result: Dict[str, Dict[str, str]] = {}
    for table_name, columns in data.items():
        if isinstance(columns, dict):
            #Фильтруем только пары " имя ->  тип"
            result[table_name] = {
                str(col_name): str(col_type) for col_name, col_type in columns.items()
            }
    return result

def save_metadata(metadata: Dict[str, Dict[str, str]]) -> None:
    """
    Сохраняем метаданные в json.
    """
    with METADATA_FILE.open("w", encoding="utf-8")  as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

def delete_table_data(table_name: str) -> None:
    """
    Удаляет json файл содержимого таблицы, если удаляется сама таблица.
    """
    table_path = DATA_DIR / f"{table_name}.json"
    if table_path.exists():
        table_path.unlink()

def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    """
    Загружаем данные таблицы из файла *.json.
    Если файйа нет или он поврежден возвращаем пустой список.
    """
    path = DATA_DIR / f"{table_name}.json"

    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data: Any = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    
    if not isinstance(data, list):
        return []
    
    result: List[Dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            result.append(row)
    return result

def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Сохраняем данные таблицы в файл json
    """
    if not DATA_DIR.exists():
        DATA_DIR.mkdir()

    path = DATA_DIR / f"{table_name}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)