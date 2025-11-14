# src/primitive_db/utils.py

"""
Вспомогательные функции для работы с файлом метаданных базы данных.
"""

import json
from pathlib import Path
from typing import Any, Dict

# Файл, в котором храним описание таблиц.

METADATA_FILE = Path("db_meta.json")

def load_metadata() -> Dict[str, Dict[str, str]]:
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
    with METADATA_FILE.open("w", encoding="utf-8")  as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)