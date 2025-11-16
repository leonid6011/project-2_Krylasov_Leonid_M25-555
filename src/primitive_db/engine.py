# src/primitive_db/engine.py

"""
Командный интерфейс для работы с примитивной базой данных.
"""

import re
import shlex
from typing import Any, Dict, List, Optional

from prettytable import PrettyTable
from prompt import string

from .constants import HELP_INFO
from .core import (
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    update,
)
from .decorators import create_cacher
from .parser import (
    _parse_column_defs,
    _parse_set_clause,
    _parse_values_list,
    _parse_where_clause,
)
from .utils import load_metadata, load_table_data, save_metadata, save_table_data

select_cache = create_cacher()

def _print_help() -> None:
    """
    Печать "help" сообщения с коммандами
    """
    print(HELP_INFO)
   
def _print_table(
        table_name: str,
        metadata: Dict[str, Dict[str, str]],
        rows: List[Dict[str, Any]]
) -> None:
    """
    Красиво вывести записи таблицы с помощью PrettyTable.
    """
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return
    
    if not rows:
        print("Записей не найдено.")
        return
    
    columns = list(metadata[table_name].keys())
    table = PrettyTable()
    table.field_names = columns

    for row in rows:
        table.add_row([row.get(col) for col in columns])

    print(table)


def run() -> None:
    """
    Запуск основного цикла работы с бд.
    """
    metadata = load_metadata()

    print("***База данных***\n")
    _print_help()

    while True:
        try:
            raw_input_line = string(">>>Введите команду: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not raw_input_line:
            continue
        
        lower = raw_input_line.lower()
        try:
            parts = shlex.split(raw_input_line)
        except ValueError as exc:
            print(f"Ошибка разбора команды: {exc}")
            continue

        if not parts:
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
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue

            result = create_table(metadata, table_name, columns)
            if result is None:
                continue
            
            metadata, full_columns = result

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

            result = drop_table(metadata, table_name)
            if result is None:
                continue

            save_metadata(metadata)
            print(f'Таблица "{table_name}" успешно удалена.')
            continue
        
        #insert into <table> values
        if lower.startswith("insert into "):
            try:
                match = re.search(r"\bvalues\b", lower)
                if not match:
                    raise ValueError(
                        "Некорректная команда insert."
                        "Ожидается: insert into <имя_таблицы> values (<значения>)."
                    )
                values_pos = match.start()
                table_name = raw_input_line[len("insert into "):values_pos].strip()

                values_part = raw_input_line[match.end():].strip()
                if not (values_part.startswith("(") and values_part.endswith(")")):
                    raise ValueError(
                        "Некорректный формат values. "
                        "Ожидаются скобки: values (<значение1>, <значение2>, ...)."
                    )
                inner = values_part[1:-1]
                values = _parse_values_list(inner)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue

            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)
            result = insert(metadata, table_name, table_data, values)
            if result is None:
                continue

            table_data, new_id = result
            save_table_data(table_name, table_data)

            print(
                f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".'
            )
            continue

        # select from <table>
        if lower.startswith("select from "):
            try:
                where_index = lower.find(" where ")
                if where_index == -1:
                    table_name = raw_input_line[len("select from "):].strip()
                    where_clause: Optional[Dict[str, Any]] = None
                else:
                    table_name = raw_input_line[
                        len("select from "):where_index
                    ].strip()
                    where_text = raw_input_line[
                        where_index + len(" where "):
                    ].strip()
                    where_clause = _parse_where_clause(where_text)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)
            cache_key = (table_name, repr(where_clause))

            rows = select_cache(
                cache_key,
                lambda: select(table_data, where_clause)
            )
            if rows is None:
                continue
            _print_table(table_name, metadata, rows)
            continue
        
        #update <table> set
        if lower.startswith("update "):
            try:
                set_index = lower.find(" set ")
                where_index = lower.find(" where ")
                if set_index == -1 or where_index == -1 or where_index < set_index:
                    raise ValueError(
                        "Некорректная кманда update. "
                        "Ожидается: update <имя_таблицы> set ... where ... ."
                    )
                
                table_name = raw_input_line[len("update "):set_index].strip()
                set_text = raw_input_line[
                    set_index + len(" set "):where_index
                ].strip()
                where_text = raw_input_line[
                    where_index + len(" where "):
                ].strip()

                set_clause = _parse_set_clause(set_text)
                where_clause = _parse_where_clause(where_text)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue
                
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)
            result = update(table_data,  set_clause, where_clause)
            if result is None:
                continue
            table_data, updated_ids = result
            save_table_data(table_name, table_data)

            if not updated_ids:
                print("Под походящее условие не попала ни одна запись.")
            else:
                for rec_id in updated_ids:
                    print(
                        f'Запись с ID={rec_id} в таблце "{table_name}" '
                        f'успешно обновлена'
                    )
            continue

        #delete from <table> where
        if lower.startswith("delete from "):
            try:
                where_index = lower.find(" where ")
                if where_index == -1:
                    raise ValueError(
                        "Некорректная команда delete. "
                        "Ожидается: delete from <имя_таблицы> where "
                        "<столбец> = <значение>."
                    )
                
                table_name = raw_input_line[len("delete from "):where_index].strip()
                where_text = raw_input_line[
                    where_index + len(" where "):
                ].strip()
                where_clause = _parse_where_clause(where_text)
            except ValueError as exc:
                print(f"Ошибка: {exc}")
                continue

            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            table_data = load_table_data(table_name)

            result = delete(table_data, where_clause)
            if result is None:
                continue

            new_data, deleted_ids = result
            save_table_data(table_name, new_data)

            if not deleted_ids:
                print("Под подходящее условие не попала и одна запись.")
            else:
                for rec_id in deleted_ids:
                    print(
                        f'Запись с ID={rec_id} успешно удалена из таблицы '
                        f'"{table_name}".'
                    )
            continue
        
        # info <table>
        if lower.startswith("info "):
            table_name = raw_input_line[len("info "):].strip()
            if not table_name:
                print("Ошибка: не указано имя таблицы.")
                continue

            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            columns = metadata[table_name]
            cols_str = ", ".join(
                f"{name}:{col_type}" for name, col_type in columns.items()
            )
            table_data = load_table_data(table_name)
            print(f"Таблица: {table_name}")
            print(f"Столбцы: {cols_str}")
            print(f"Количество записей: {len(table_data)}")
            continue

        #unknown_command
        print(
            "Ошибка: неизвестная команда.\n"
            'Введите "help" для просмотра доступных команд.'
        )
