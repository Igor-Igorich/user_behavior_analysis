'''Вспомогательные утилиты:
- настройка логирования
- проверка наличия колонок в DataFrame
- создание директорий
- форматирование чисел и дат
'''

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Union

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    '''Создаёт и настраивает логгер для вывода информации в консоль.
        
    Аргументы:
        name (str): Имя логгера (обычно __name__ из вызывающего модуля)
        level (int): Уровень логирования (по умолчанию = logging.INFO)
        
    Возвращает:
        logging.Logger: Настроенный логгер
    '''
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger

def validate_columns(df, required_cols: List[str], logger=None) -> bool:
    '''Проверяет, что в DataFrame присутствуют все необходимые колонки.
    
    Аргументы:
        df (pd.DataFrame): Проверяемый DataFrame
        required_cols (List[str]): Список обязательных названий колонок
        logger (logging.Logger, optional): Логгер для записи ошибок (по умолчанию = None)
        
    Возвращает:
        bool: True если все колонки есть, False если хотя бы одной нет
    '''
    
    required_set = set(required_cols)
    missed = required_set - set(df.columns)
    
    if missed:
        msg = f'Отсутствуют обязательные колонки: {missed}'
        if logger:
            logger.error(msg)
        else:
            print(msg)
        return False
    
    if logger:
        logger.info(f'Все колонки присутствуют: {required_cols}')
    return True

def ensure_dir(path: Union[str, Path]) -> Path:
    '''Создаёт директорию, если она не существует.
    
    Аргументы:
        path (str или Path): Путь к директории, которую нужно создать
        
    Возвращает:
        Path: Объект Path созданной директории
    '''
    if isinstance(path, str):
        path = Path(path)
    
    path.mkdir(parents=True, exist_ok=True)
    
    return path

def format_number(num: Union[int, float]) -> str:
    '''Форматирование больших чисел с разделителями тысяч.
    
    Аргументы:
        num (int или float): Число для форматирования
        
    Возвращает:
        str: Отформатированная строка
    '''
    if isinstance(num, float):
        return f'{num:,.2f}'
    else:
        return f'{num:,}'


def format_percentage(value: float, total: float = None, decimals: int = 1) -> str:
    '''Форматирование чисел как процентов.
    
    Аргументы:
        value (float): Число
        total (float, optional): Общее значение (по умолчанию = None)
        decimals (int): Количество знаков после запятой (по умолчанию = 1)
        
    Возвращает:
        str: Процент с символом %
    '''
    if total:
        res = value / total
    
    return f'{res * 100:.{decimals}f}%'
    
def timer(logger=None):
    '''Декоратор для измерения времени выполнения функции.
    
    Аргументы:
        logger (logging.Logger, optional): Логгер для записи времени (по умолчанию = None)
    '''
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            elapsed  = (datetime.now() - start_time).total_seconds()
            
            msg = f'Функция {func.__name__} выполнилась за {elapsed:.2f} сек'
            if logger:
                logger.info(msg)
            else:
                print(msg)
            
            return result
        return wrapper
    return decorator

def quick_info(df, logger=None, sample_rows: int = 3):
    """
    Выводит краткую информацию о DataFrame для быстрой отладки (альтернатива df.info() + df.head() + df.describe()).
    
    Аргументы:
        df (pd.DataFrame): Исследуемый DataFrame
        logger (logging.Logger, optional): Логгер (по умолчанию = None)
        sample_rows (int): Сколько строк показать в примере (по умолчанию = 3)
    """
    if logger:
        logger.info(f"Форма DataFrame: {df.shape[0]:,} строк x {df.shape[1]} колонок")
        logger.info(f"Колонки: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")

        dtype_counts = df.dtypes.value_counts()
        type_summary = ', '.join([f"{k}({v})" for k, v in dtype_counts.items()])
        logger.info(f"Типы данных: {type_summary}")

        missing = df.isnull().sum()
        missing_cols = missing[missing > 0]
        if len(missing_cols) > 0:
            logger.warning(f"Пропуски: {', '.join([f'{col} ({val})' for col, val in missing_cols.items()])}")
        else:
            logger.info("Нет пропусков в данных")
    else:
        print(f"Форма: {df.shape}")
        print(f"Колонки: {list(df.columns)}")
        print(f"Типы:\n{df.dtypes.value_counts()}")
        
        missing = df.isnull().sum()
        if (missing > 0).any():
            print(f"Пропуски:\n{missing[missing > 0]}")

    print(f"\nПервые {sample_rows} строк:")
    print(df.head(sample_rows))
