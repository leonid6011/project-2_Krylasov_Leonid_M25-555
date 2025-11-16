# src/primitive_db/decorators.py

from __future__ import annotations

import time
from functools import wraps
from typing import Any, Callable, Dict, Hashable, Tuple

Func = Callable[..., Any]

def handle_db_errors(func: Func) -> Func:
    """
    Централизованная обработка ошибок для DB-функций.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: файл данных не найден."
                "Возможно, база данных ещё не инициализирована."
            )
        except KeyError as exc:
            print(f"Ошибка: таблица или столбец {exc} не найден.")
        except ValueError as exc:
            print(f"Ошибка: {exc}")
        except Exception as exc:
            print(f"Произошла непредвиденная ошибка: {exc}")
    
    return wrapper

def confirm_action(action_name: str) -> Callable[[Func], Func]:
    """
    Декораторы для подтверждения опасных ситуаций.
    """
    def decorator(func: Func) -> Func:
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]:'
            )
            if answer.strip().lower() != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator

def log_time(func: Func) -> Func:
    """
    Логгирует время выполнения функции.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.6f} секунд.")
        return result

    return wrapper

def create_cacher() -> Tuple[
    Callable[[Hashable, Callable[[], Any]], Any],
    Callable[[], None]
]:
    """
    Создание функции для кеширования результатов
    """
    cache: Dict[Hashable, Any] = {}

    def cache_result(key: Hashable, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value
    
    def clear_cache() -> None:
        cache.clear()
    
    return cache_result, clear_cache