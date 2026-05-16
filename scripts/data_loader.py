"""
Модуль загрузки и предобработки данных событий.

Этот модуль содержит функции для:
- загрузки сырых данных из CSV
- валидации и очистки данных
- преобразования временных типов и создания временных признаков
- сохранения обработанных данных
"""

import pandas as pd
from pathlib import Path
from typing import Union, List, Optional

from .utils import setup_logger, validate_columns, ensure_dir, timer

logger = setup_logger(__name__)

def load_raw_events(filepath: Union[str, Path]) -> pd.DataFrame:
    '''
    Загружает сырые данные о событиях из CSV-файла.
    
    Аргументы:
        filepath (str или Path): Путь к файлу с данными
        
    Возвращает:
        pd.DataFrame: DataFrame с сырыми данными
        
    Raises:
        FileNotFoundError: Если файл не существует
        ValueError: Если расширение файла не поддерживается
    '''
    if isinstance(filepath, str):
        filepath = Path(filepath)
    
    if not filepath.exists:
        raise FileNotFoundError(f'Файл не найден: {filepath}')
    
    suffix = filepath.suffix.lower()
    if suffix != '.csv':
        raise ValueError(f'Неподдерживаемый формат файла: {suffix}. Используйте .csv')
    
    logger.info(f'Начинаем загрузку данных из {filepath}')
    df = pd.read_csv(filepath)
    logger.info(f'Загружено {df.shape[0]} строк, {df.shape[1]} колонок')
    
    return df

def preprocess_events(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Выполняет предобработку DataFrame.
    
    Этапы обработки:
    1. Проверка наличия обязательных колонок
    2. Преобразование времени в datetime
    3. Создание временных признаков (дата, час, день недели)
    4. Удаление дубликатов
    5. Обработка пропущенных значений
    6. Фильтрация выбросов (опционально)
    
    Аргументы:
        df (pd.DataFrame): Исходный DataFrame
        
    Возвращает:
        pd.DataFrame: Очищенный DataFrame
    '''
    
    logger.info('Начало предобработки данных.')
    
    df_clean = df.copy()
    
    required_cols = [
        'user_id', 'session_id', 'event_date', 'event_time',
        'event_type', 'product_id', 'category', 'price', 'device',
        'traffic_source', 'country', 'city'
    ]
    if not validate_columns(df_clean, required_cols, logger):
        raise ValueError('Отсутствуют обязательные колонки. Проверьте структуру данных.')
    
    logger.info(f'Исходный размер: {df_clean.shape[0]} строк')
    
    logger.info('Преобразование временных меток...')
    df_clean['event_time'] = pd.to_datetime(
        df_clean['event_time'] + ' ' + df_clean['event_date'],
        errors='coerce'
    )
    df_clean = df_clean.drop('event_date', axis=1)
    
    invalid_dates = df_clean['event_time'].isna().sum()
    if invalid_dates:
        logger.warning(f'Найдено {invalid_dates} строк с некорректными датами. Они будут удалены.')
        df_clean = df_clean.dropna(subset=['event_time'])
    
    logger.info('Добавление временных признаков...')
    
    def get_time_of_day(hour: int) -> str:
        '''Определяет время суток по часу'''
        if 5 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 23:
            return 'evening'
        else:
            return 'night'
    
    df_clean['date'] = df_clean['event_time'].dt.date
    df_clean['hour'] = df_clean['event_time'].dt.hour
    df_clean['weekday'] = df_clean['event_time'].dt.day_of_week
    df_clean['month'] = df_clean['event_time'].dt.month
    df_clean['year'] = df_clean['event_time'].dt.year
    
    df_clean['time_of_day'] = df_clean['hour'].apply(get_time_of_day)
    
    
    before_dedup = df_clean.shape[0]
    df_clean = df_clean.drop_duplicates()
    cnt_dup = before_dedup - df_clean.shape[0]
    
    if cnt_dup:
        logger.info(f'Удалено {cnt_dup} полных дубликатов')
    
    
    logger.info('Предобработка данных завершена.')
    logger.info(f'Итоговый размер: {df_clean.shape[0]} строк')
    
    return df_clean

def save_processed_data(df: pd.DataFrame, output_path: Union[str, Path]) -> None:
    '''
    Сохраняет обработанный DataFrame в файл.
    
    Аргументы:
        df (pd.DataFrame): Данные для сохранения
        output_path (str или Path): Путь для сохранения
    '''
    
    if isinstance(output_path, str):
        output_path = Path(output_path)
    
    ensure_dir(output_path.parent)
    df.to_csv(output_path, index=False)
    logger.info(f'Сохранено в {output_path}')
    
@timer(logger)
def make_processed_data(
    raw_path: Union[str, Path],
    processed_path: Union[str, Path]
) -> pd.DataFrame:
    '''
    Главная функция, которая запускает пайплайн обработки данных.

    Аргументы:
        raw_path (str или Path): Путь к сырым данным
        processed_path (str или Path): Путь для сохранения обработанных данных
        
    Возвращает:
        pd.DataFrame: Обработанный DataFrame
    '''
    
    logger.info('Запуск пайплайна обработки данных.')
    
    df_raw = load_raw_events(raw_path)
    df_clean = preprocess_events(df_raw)
    save_processed_data(df_clean, processed_path)
    
    logger.info('Пайплайн успешно завершен.')
    
    return df_clean
    
    