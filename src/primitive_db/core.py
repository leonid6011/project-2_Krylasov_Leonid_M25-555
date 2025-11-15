# src/primitive_db/core.py

"""
Ядро примитивной СУБД
"""


from typing import Any, Dict, List, Optional, Tuple

from .decorators import confirm_action, handle_db_errors, log_time

# Допустимые типы столбцов
VALID_TYPES = {"int", "str", "bool"}

# Допустимые типы аннотаций
Metadata = Dict[str, Dict[str, str]]
ColumnDef = Tuple[str, str]
Row = Dict[str, Any]

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

@handle_db_errors
def create_table(
    metadata: Metadata,
    table_name: str,
    columns: List[ColumnDef]
    ) -> List[ColumnDef]:
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

@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata: Metadata, table_name: str) -> None:
    """
    Удаляем таблицу из метаданных.
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    del metadata[table_name]

def list_tables(metadata: Metadata) -> List[str]:
    """
    Возвращаем список всех таблиц.
    """
    return list(metadata.keys())

def _row_matches(row: Row, where_clause: Optional[Dict[str, Any]]) -> bool:
    """
    Проверяет, удовлетворяет ли строка условию where.
    """
    if not where_clause:
        return True
    
    for key, value in where_clause.items():
        if row.get(key) != value:
            return False
    return True

@log_time
@handle_db_errors
def insert(
        metadata: Metadata,
        table_name: str,
        table_data: List[Row],
        values: List[Any]
) -> Tuple[List[Row], int]:
    """
    Добавить новую запись в таблицу. 
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    schema = metadata[table_name]
    column_names = list(schema.keys())
    non_id_columns = column_names[1:]

    if len(values) != len(non_id_columns):
        raise ValueError(
            f"Ожидается {len(non_id_columns)} значений, получено {len(values)}."
        )
    
    # Проверяем тип значений
    for col_name, value in zip(non_id_columns, values):
        expected_type = schema[col_name]
        if expected_type == "int" and not isinstance(value, int):
            raise ValueError(
                f'Некорректный тип для столбца "{col_name}": ожидается int.'
            )
        if expected_type == "str" and not isinstance(value, str):
            raise ValueError(
                f'Некорректный тип для столбца "{col_name}": ожидается str.'
            )
        if expected_type == "bool" and not isinstance(value, bool):
            raise ValueError(
                f'Некорректный тип для столбца "{col_name}": ожидается bool.'
            )
    
    # Генерация ID
    if table_data:
        existing_ids = [
            row.get("ID", 0)
            for row in table_data
            if isinstance(row.get("ID"), int)
        ]
        new_id = (max(existing_ids) + 1 if existing_ids else 1)
    else:
        new_id = 1

    new_row: Row = {"ID": new_id}
    for col_name, value in zip(non_id_columns, values):
        new_row[col_name] = value

    table_data.append(new_row)
    return table_data, new_id

@log_time
@handle_db_errors
def select(
        table_data: List[Row],
        where_clause: Optional[Dict[str, Any]] = None
) -> List[Row]:
    """
    Вернуть список записей, удовлетворяющих where_clause.
    Если where_clause не задан, возвращает все записи.
    """
    return [row for row in table_data if _row_matches(row, where_clause)]

@handle_db_errors
def update(
        table_data: List[Row],
        set_clause: Dict[str, Any],
        where_clause: Optional[Dict[str, Any]]
) -> Tuple[List[Row], List[int]]:
    """
    Обновить записи по условию where_clause согласно set_clause.
    """
    updated_ids: List[int] = []

    for row in table_data:
        if _row_matches(row, where_clause):
            for key, value in set_clause.items():
                if key not in row:
                    raise ValueError(
                        f'Столбец "{key}" не существует в таблице.'
                    )
                row[key] = value
            if isinstance(row.get("ID"), int):    
                updated_ids.append(row["ID"])

    return table_data, updated_ids

@confirm_action("удаление записей")
@handle_db_errors
def delete(
        table_data: List[Row],
        where_clause: Optional[Dict[str, Any]]
) -> Tuple[List[Row], List[int]]:
    """
    Удаляет записи по условию where_cause.
    """
    remaining: List[Row] = []
    deleted_ids: List[int] = []

    for row in table_data:
        if _row_matches(row, where_clause):
            if isinstance(row.get("ID"), int):
                deleted_ids.append(row["ID"])
        else:
            remaining.append(row)
    return remaining, deleted_ids
