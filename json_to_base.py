"""
json_to_base.py - Загрузка JSON данных в PostgreSQL и создание векторной базы ChromaDB
"""

import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import chromadb
from chromadb.config import Settings
import ollama
import logging
import numpy as np
from typing import Dict, Any, List
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('JSONToBase')

# Параметры подключения к PostgreSQL (из переменных окружения или по умолчанию)
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'video_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

def read_json_file(file_path: str) -> Dict[str, Any]:
    """Чтение JSON-файла с обработкой ошибок"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"✅ Успешно прочитан JSON-файл: {file_path}")
        logger.info(f"   Найдено видео: {len(data.get('videos', []))}")
        return data
    except FileNotFoundError:
        logger.error(f"❌ Файл не найден: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка парсинга JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка чтения файла: {e}")
        raise

def create_database_if_not_exists():
    """Создание базы данных, если она не существует"""
    try:
        # Подключаемся к postgres для создания БД
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Проверяем существование БД
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            logger.info(f"✅ База данных '{DB_CONFIG['database']}' создана")
        else:
            logger.info(f"ℹ️  База данных '{DB_CONFIG['database']}' уже существует")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка создания БД: {e}")
        raise

def create_and_populate_database(data: Dict[str, Any]) -> None:
    """Создание и заполнение PostgreSQL базы данных"""
    try:
        # Создаем БД если нужно
        create_database_if_not_exists()
        
        # Подключаемся к целевой БД
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        
        logger.info("🔨 Создание таблиц...")
        
        # Удаляем существующие таблицы (для чистоты)
        cursor.execute("DROP TABLE IF EXISTS video_snapshots CASCADE")
        cursor.execute("DROP TABLE IF EXISTS videos CASCADE")
        
        # Создание таблицы videos
        cursor.execute('''
        CREATE TABLE videos (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            video_created_at TIMESTAMP NOT NULL,
            views_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reports_count INTEGER DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        ''')
        
        # Создание индексов для videos
        cursor.execute("CREATE INDEX idx_videos_creator ON videos(creator_id)")
        cursor.execute("CREATE INDEX idx_videos_created_at ON videos(video_created_at)")
        cursor.execute("CREATE INDEX idx_videos_views ON videos(views_count)")
        
        # Создание таблицы video_snapshots
        cursor.execute('''
        CREATE TABLE video_snapshots (
            id TEXT PRIMARY KEY,
            video_id TEXT NOT NULL,
            views_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reports_count INTEGER DEFAULT 0,
            delta_views_count INTEGER DEFAULT 0,
            delta_likes_count INTEGER DEFAULT 0,
            delta_comments_count INTEGER DEFAULT 0,
            delta_reports_count INTEGER DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
        )
        ''')
        
        # Создание индексов для video_snapshots
        cursor.execute("CREATE INDEX idx_snapshots_video_id ON video_snapshots(video_id)")
        cursor.execute("CREATE INDEX idx_snapshots_created_at ON video_snapshots(created_at)")
        cursor.execute("CREATE INDEX idx_snapshots_delta_views ON video_snapshots(delta_views_count)")
        
        logger.info("✅ Таблицы созданы")
        
        # Заполнение таблицы videos
        logger.info("📝 Заполнение таблицы videos...")
        videos_to_insert = []
        for video in data['videos']:
            videos_to_insert.append((
                video['id'],
                video['creator_id'],
                video['video_created_at'],
                video.get('views_count', 0),
                video.get('likes_count', 0),
                video.get('comments_count', 0),
                video.get('reports_count', 0),
                video.get('created_at', datetime.now().isoformat()),
                video.get('updated_at', datetime.now().isoformat())
            ))
        
        execute_batch(cursor, '''
        INSERT INTO videos (
            id, creator_id, video_created_at, views_count, likes_count, 
            comments_count, reports_count, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', videos_to_insert)
        
        logger.info(f"✅ Добавлено {len(videos_to_insert)} видео")
        
        # Заполнение таблицы video_snapshots
        logger.info("📝 Заполнение таблицы video_snapshots...")
        snapshots_to_insert = []
        for video in data['videos']:
            for snapshot in video.get('snapshots', []):
                snapshots_to_insert.append((
                    snapshot['id'],
                    video['id'],
                    snapshot.get('views_count', 0),
                    snapshot.get('likes_count', 0),
                    snapshot.get('comments_count', 0),
                    snapshot.get('reports_count', 0),
                    snapshot.get('delta_views_count', 0),
                    snapshot.get('delta_likes_count', 0),
                    snapshot.get('delta_comments_count', 0),
                    snapshot.get('delta_reports_count', 0),
                    snapshot['created_at'],
                    snapshot.get('updated_at', datetime.now().isoformat())
                ))
        
        execute_batch(cursor, '''
        INSERT INTO video_snapshots (
            id, video_id, views_count, likes_count, comments_count, reports_count,
            delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', snapshots_to_insert, page_size=1000)
        
        logger.info(f"✅ Добавлено {len(snapshots_to_insert)} снапшотов")
        
        conn.commit()
        logger.info("✅ PostgreSQL база данных успешно создана и заполнена")
        
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка работы с PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def generate_schema_embedding(model: str = 'nomic-embed-text-v2-moe') -> np.ndarray:
    """Генерация эмбеддинга для схемы базы данных"""
    schema_description = """
    База данных видео-аналитики содержит две таблицы:
    
    Таблица VIDEOS (итоговая статистика по видео):
    - id: уникальный идентификатор видео
    - creator_id: идентификатор создателя контента
    - video_created_at: дата и время публикации видео (TIMESTAMP)
    - views_count: финальное количество просмотров
    - likes_count: финальное количество лайков
    - comments_count: финальное количество комментариев
    - reports_count: финальное количество жалоб
    - created_at, updated_at: служебные поля
    
    Таблица VIDEO_SNAPSHOTS (почасовые замеры метрик):
    - id: уникальный идентификатор снапшота
    - video_id: ссылка на videos.id
    - views_count, likes_count, comments_count, reports_count: текущие значения на момент замера
    - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count: приращения с прошлого замера
    - created_at: время замера (TIMESTAMP, раз в час)
    - updated_at: служебное поле
    
    ВАЖНЫЕ ПАТТЕРНЫ ЗАПРОСОВ:
    - Для подсчета общего количества видео: SELECT COUNT(*) FROM videos
    - Для фильтрации по креатору: WHERE creator_id = 'xxx'
    - Для фильтрации по датам публикации: WHERE video_created_at BETWEEN 'date1' AND 'date2'
    - Для фильтрации по просмотрам: WHERE views_count > threshold
    - Для анализа роста за дату: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'date'
    - Для подсчета видео с активностью: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = 'date' AND delta_views_count > 0
    
    Все даты хранятся в формате TIMESTAMP, используйте DATE() для извлечения только даты.
    Для диапазонов дат используйте BETWEEN или >= AND <=.
    """
    
    try:
        response = ollama.embeddings(model=model, prompt=schema_description)
        return np.array(response['embedding'], dtype=np.float32)
    except Exception as e:
        logger.error(f"❌ Ошибка генерации эмбеддинга схемы: {e}")
        raise

def create_embedding_database(collection_name: str = 'video_schema') -> int:
    """
    Создание компактной векторной базы ChromaDB только для СХЕМЫ БД
    (не для каждого видео - это избыточно)
    """
    try:
        logger.info("🔨 Создание ChromaDB коллекции...")
        
        # Инициализация ChromaDB
        client = chromadb.PersistentClient(
            path="./chroma_db",
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Удаление существующей коллекции
        try:
            client.delete_collection(name=collection_name)
            logger.info(f"🗑️  Существующая коллекция '{collection_name}' удалена")
        except Exception:
            pass
        
        # Создание новой коллекции
        collection = client.create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        
        # Генерация эмбеддинга схемы
        logger.info("🧠 Генерация эмбеддинга для схемы БД...")
        schema_embedding = generate_schema_embedding()
        
        # Документация схемы
        schema_doc = """
Схема базы данных видео-аналитики с примерами запросов.

ТАБЛИЦЫ:
1. videos - основная информация о видео
2. video_snapshots - почасовые снимки метрик с дельтами

ТИПОВЫЕ ЗАПРОСЫ:
- Общее количество: COUNT(*) FROM videos
- По креатору: WHERE creator_id = 'ID'
- По дате публикации: WHERE DATE(video_created_at) BETWEEN 'date1' AND 'date2'
- По просмотрам: WHERE views_count > N
- Рост за дату: SUM(delta_*_count) FROM video_snapshots WHERE DATE(created_at) = 'date'
- Уникальные видео с активностью: COUNT(DISTINCT video_id) WHERE DATE(created_at) = 'date' AND delta_views_count > 0
        """
        
        # Добавление в ChromaDB
        collection.add(
            embeddings=[schema_embedding.tolist()],
            documents=[schema_doc],
            metadatas=[{"type": "database_schema"}],
            ids=["schema_v1"]
        )
        
        final_count = collection.count()
        logger.info(f"✅ ChromaDB коллекция '{collection_name}' создана с {final_count} записью")
        return final_count
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания ChromaDB: {e}")
        raise

def main() -> int:
    """Основная функция обработки"""
    try:
        # Путь к JSON-файлу
        json_file_path = 'video_data.json'
        
        logger.info("=" * 60)
        logger.info("ЗАГРУЗКА ДАННЫХ В БАЗУ")
        logger.info("=" * 60)
        
        # Чтение данных
        data = read_json_file(json_file_path)
        
        # Проверка структуры
        if 'videos' not in data or not isinstance(data['videos'], list):
            raise ValueError("❌ Некорректная структура JSON: отсутствует 'videos'")
        
        logger.info(f"📊 Загружено видео: {len(data['videos'])}")
        
        # Подсчет снапшотов
        total_snapshots = sum(len(v.get('snapshots', [])) for v in data['videos'])
        logger.info(f"📊 Всего снапшотов: {total_snapshots}")
        
        # Создание и заполнение PostgreSQL
        create_and_populate_database(data)
        
        # Создание векторной базы (только схема)
        create_embedding_database()
        
        logger.info("=" * 60)
        logger.info("✅ ВСЕ ЭТАПЫ УСПЕШНО ЗАВЕРШЕНЫ!")
        logger.info("=" * 60)
        logger.info(f"PostgreSQL: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
        logger.info(f"ChromaDB: ./chroma_db")
        return 0
        
    except Exception as e:
        logger.exception(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)