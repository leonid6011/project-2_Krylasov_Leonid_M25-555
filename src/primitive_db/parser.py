# src/primitive_db/parser.py

from typing import Any, Dict, List, Tuple

from .constants import VALID_TYPES


def _parse_column_defs(raw_columns: List[str]) -> List[Tuple[str, str]]:
    """
    Разбор аргументов формата <имя:тип>.
    """
    columns: List[Tuple[str, str]] = []

    for raw in raw_columns:
        if ":" not in raw or raw.count(":") != 1:
            raise ValueError(
                f'Некорректное определение столбца "{raw}".'
                'Ожидается формат "<имя:тип>".'
            )
        name, type_name = raw.split(":", 1) 
        name = name.strip()
        type_name = type_name.strip()

        if not name or not type_name:
            raise ValueError(
                f'Некорректное определение столбца "{raw}".'
                'Ожидается формат "<имя:тип>".'
            )
        if type_name not in VALID_TYPES:
            raise ValueError(
                f'Недопустимый тип "{type_name}" для столбца "{name}". '
                f'Разрешенные типы: {", ".join(sorted(VALID_TYPES))}.'
            )
        columns.append((name, type_name))
    return columns

def _parse_value(raw: str) -> Any:
    """
    Преобразование строки в значение
    """
    raw = raw.strip()

    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        return raw[1:-1]
    
    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    
    try:
        return int(raw)
    except ValueError:
        raise (f"Не удалось распознать значение {raw!r}.")
    

def _parse_values_list(text: str) -> List[Any]:
    """
    Разобрать список значений для insert
    """
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        raise ValueError("Список значений не может быть пустым.")
    return [_parse_value(part) for part in parts]

def _parse_where_clause(text: str) -> Dict[str, Any]:
    """
    Разобрать условие where
    """ 
    if "=" not in text:
        raise ValueError(
            'Некорректное условие where. Ожидается "<столбец> = <значение>".'
        )
    column, value_str = text.split("=", 1)
    column = column.strip()
    if not column:
        raise ValueError("Имя столбца в условии where не может быть пустым.")
    
    value = _parse_value(value_str)
    return {column: value}

def _parse_set_clause(text: str) -> Dict[str, Any]:
    """
    Разобрать выражение set
    """
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        raise ValueError("Выражение set не может быть пустым.")
    
    result: Dict[str, Any] = {}
    for part in parts:
        if "=" not in part:
            raise ValueError(
                'Некорректное выражение set. Ожидается "<столбец> = <значение>".'
            )
        column, value_str = part.split("=", 1)
        column = column.strip()
        if not column:
            raise ValueError("Имя столбца в выражении set не может быть пустым.")
        result[column] = _parse_value(value_str)
    return result