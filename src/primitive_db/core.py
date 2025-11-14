# src/primitive_db/core.py

"""
Ядро примитивной СУБД: функции для работы с таблицами.
"""


from typing import Dict, List, Tuple

VALID_TYPES = {"int", "str", "bool"}

Metadata = Dict[str, Dict[str, str]]
ColumnDef = Tuple[str, str]

def _validate_columns(columns: List[ColumnDef]) -> None:
    """
    Проверяет корректност определения столбцов.
    Выбрасывает ValueError, если что-то неверно.
    """
    if not columns:
        raise ValueError("Не указано ни одного столбца для таблицы.")
    
    seen = set()
    for name, col_type in columns:
        if not name:
            raise ValueError("Имя столбца не может быть пустым.")
        if name in seen:
            raise ValueError(f'Столбец "{name}" указан несколько раз.')
        if name == "ID":
            raise ValueError("Столбец 'ID' добавляется автоматически.")
        if col_type not in VALID_TYPES:
            raise ValueError(
                f'Недопустимый тип "{col_type}" для столбца "{name}".'
                f"Разрешенные типы: {', '.join(sorted(VALID_TYPES))}"
            )
        seen.add(name)

def create_table(metadata: Metadata, table_name: str, columns: List[ColumnDef]):
    """
    Слздаем таблицу в базе данных.
    Возвращает полный список столбцов.
    """
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    
    _validate_columns(columns)

    full_columns = [("ID", "int"), *columns]

    metadata[table_name] = {name: col_type for name, col_type in full_columns}
    return full_columns

def drop_table(metadata: Metadata, table_name: str) -> None:
    """
    Удаляем таблицу из метаданных
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    del metadata[table_name]

def list_tables(metadata: Metadata) -> List[str]:
    """
    Возвращаем список всех таблиц.
    """
    return list(metadata.keys())
