# src/primitive_db/engine.py

"""
Командный интерфейс для работы с примитивной базой данных.
"""

import shlex
from typing import List, Tuple

from prompt import string

from .core import VALID_TYPES, create_table, drop_table, list_tables
from .utils import load_metadata, save_metadata


def _print_help() -> None:
    """
    Prints the help message for the current mode.
    """
   
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def _parse_column_defs(raw_columns: List[str]) -> List[Tuple[str, str]]:
    """
    Разбор аргументов формата <имя:тип>.
    """
    columns = []
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
            raise ValueError(f'Некорректное определение столбца "{raw}".')
        
        if type_name not in VALID_TYPES:
            raise ValueError(
                f'Недопустимый тип "{type_name}" для столбца "{name}". '
                f'Разрешенные типы: {", ".join(sorted(VALID_TYPES))}.'
            )
        columns.append((name, type_name))
    return columns

def run() -> None:
    """
    Запуск основного цикла команной строки.
    """
    metadata = load_metadata()

    print("***База данных***\n")
    _print_help()

    while True:
        raw_input_line = string(">>>Введите команду: ").strip()

        if not raw_input_line:
            continue

        try:
            parts = shlex.split(raw_input_line)
        except ValueError as exc:
            print(f"Ошибка разбора команды: {exc}")
            continue

        command = parts[0]

        #exit
        if command == "exit":
            break

        #help
        if command == "help":
            _print_help()
            continue
        
        #list_tables
        if command == "list_tables":
            tables = list_tables(metadata)
            if not tables:
                print("Таблиц пока нет.")
            else:
                for name in tables:
                    print(f"- {name}")
                continue
        #create_table
        if command == "create_table":
            if len(parts) < 3:
                print(
                    "Ошибка: недостаточно аргументов.\n"
                    "Формат: create_table <имя_таблицы> <столбец:тип> ..."
                )
                continue

            table_name = parts[1]
            raw_columns = parts[2:]

            try:
                columns = _parse_column_defs(raw_columns)
                full_columns = create_table(metadata, table_name, columns)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue
        
            save_metadata(metadata)

            cols_str = ", ".join(
                f"{name}:{type_name}" for name, type_name in full_columns
            )
            print(
                f'Таблица "{table_name}" успешно создана '
                f"со столбцами: {cols_str}"
            )
            continue
        
        #drop_table
        if command == "drop_table":
            if len(parts) != 2:
                print(
                    "Ошибка: некорректное число аргументов.\n"
                    "Формат: drop_table <имя_таблицы>"
                )
                continue
            table_name = parts[1]
            
            try:
                drop_table(metadata, table_name)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue

            save_metadata(metadata)
            print(f'Таблица "{table_name}" успешно удалена.')
            continue
        
        #unknown_command
        print(
            "Ошибка: неизвествна команда.\n"
            'Введите "help" для просмотра доступных команд.'
        )
