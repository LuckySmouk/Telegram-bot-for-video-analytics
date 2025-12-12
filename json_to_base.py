import json
import sqlite3
from datetime import datetime
import chromadb
from chromadb.errors import NotFoundError
import ollama
import logging
import numpy as np
from typing import Dict, Any, List, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('JSONToBase')

def read_json_file(file_path: str) -> Dict[str, Any]:
    """Чтение JSON-файла с обработкой ошибок"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"Успешно прочитан JSON-файл: {file_path}")
        return data
    except Exception as e:
        logger.error(f"Ошибка чтения JSON-файла: {e}")
        raise

def create_and_populate_database(data: Dict[str, Any], db_name: str = 'video_data.db') -> None:
    """Создание и заполнение SQLite базы данных"""
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Создание таблицы videos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            creator_id TEXT NOT NULL,
            video_created_at TEXT NOT NULL,
            views_count INTEGER DEFAULT 0,
            likes_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reports_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        ''')

        # Создание таблицы video_snapshots
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS video_snapshots (
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
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (video_id) REFERENCES videos (id) ON DELETE CASCADE
        )
        ''')

        current_time = datetime.now().isoformat()
        
        # Заполнение таблицы videos
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
                video.get('created_at', current_time),
                video.get('updated_at', current_time)
            ))
        
        cursor.executemany('''
        INSERT OR REPLACE INTO videos (
            id, creator_id, video_created_at, views_count, likes_count, 
            comments_count, reports_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', videos_to_insert)

        # Заполнение таблицы video_snapshots
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
                    snapshot.get('updated_at', current_time)
                ))
        
        cursor.executemany('''
        INSERT OR REPLACE INTO video_snapshots (
            id, video_id, views_count, likes_count, comments_count, reports_count,
            delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', snapshots_to_insert)

        conn.commit()
        logger.info(f"База данных SQLite успешно создана и заполнена. Видео: {len(videos_to_insert)}, Снимки: {len(snapshots_to_insert)}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка работы с SQLite: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def generate_embeddings(texts: List[str], model: str = 'nomic-embed-text-v2-moe') -> List[np.ndarray]:
    """Генерация эмбеддингов с использованием Ollama и преобразование в numpy массивы"""
    try:
        embeddings = []
        for i, text in enumerate(texts):
            response = ollama.embeddings(model=model, prompt=text)
            # Преобразуем эмбеддинг в numpy массив с типом float32
            embedding_array = np.array(response['embedding'], dtype=np.float32)
            embeddings.append(embedding_array)
            if i % 10 == 0:
                logger.info(f"Сгенерировано эмбеддингов: {i+1}/{len(texts)}")
        logger.info(f"Успешно сгенерировано {len(embeddings)} эмбеддингов")
        return embeddings
    except Exception as e:
        logger.error(f"Ошибка генерации эмбеддингов: {e}")
        raise

def create_embedding_database(data: Dict[str, Any], collection_name: str = 'video_embeddings') -> int:
    """Создание базы эмбеддингов в ChromaDB и возврат количества записей"""
    try:
        # Инициализация ChromaDB
        client = chromadb.PersistentClient(path="./chroma_db")
        
        # Удаление существующей коллекции для чистоты (если она существует)
        try:
            client.delete_collection(name=collection_name)
            logger.info(f"Существующая коллекция '{collection_name}' удалена")
        except (ValueError, NotFoundError):
            # Обрабатываем случаи, когда коллекции нет
            logger.info(f"Коллекция '{collection_name}' не существует или уже удалена. Создаем новую.")
        
        # Создание новой коллекции
        collection = client.create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"},
            embedding_function=None  # Используем внешние эмбеддинги
        )
        
        logger.info("Подготовка данных для ChromaDB...")
        
        # Подготовка данных
        documents = []
        metadatas = []
        ids = []
        
        for video in data['videos']:
            # Формируем информативное описание для эмбеддингов
            description = (
                f"Видео ID: {video['id']}\n"
                f"Создатель: {video['creator_id']}\n"
                f"Дата создания: {video['video_created_at']}\n"
                f"Просмотры: {video.get('views_count', 0)}\n"
                f"Лайки: {video.get('likes_count', 0)}\n"
                f"Комментарии: {video.get('comments_count', 0)}\n"
                f"Жалобы: {video.get('reports_count', 0)}"
            )
            
            # Добавляем информацию о снимках
            if 'snapshots' in video and video['snapshots']:
                latest_snapshot = video['snapshots'][-1]
                description += (
                    f"\nПоследний снимок ({latest_snapshot['created_at']}):\n"
                    f"Дельта просмотров: {latest_snapshot.get('delta_views_count', 0)}\n"
                    f"Дельта лайков: {latest_snapshot.get('delta_likes_count', 0)}"
                )
            
            documents.append(description)
            metadatas.append({
                "video_id": video['id'],
                "creator_id": video['creator_id'],
                "type": "video_metadata"
            })
            ids.append(f"video_{video['id']}")

        # Генерация эмбеддингов
        logger.info("Генерация эмбеддингов для данных...")
        embeddings = generate_embeddings(documents)
        
        # Убедимся, что все эмбеддинги имеют одинаковую размерность
        if embeddings:
            dim = embeddings[0].shape[0]
            logger.info(f"Размерность эмбеддингов: {dim}")
        else:
            raise ValueError("Не удалось сгенерировать эмбеддинги")
        
        # Добавление в коллекцию небольшими партиями
        batch_size = 50
        total_added = 0
        
        for i in range(0, len(documents), batch_size):
            batch_docs = documents[i:i+batch_size]
            batch_embeddings = embeddings[i:i+batch_size]
            batch_metadatas = metadatas[i:i+batch_size]
            batch_ids = ids[i:i+batch_size]
            
            # Конвертируем эмбеддинги в правильный формат для ChromaDB
            embeddings_to_add = [emb.tolist() for emb in batch_embeddings]
            
            collection.add(
                embeddings=embeddings_to_add,
                documents=batch_docs,
                metadatas=batch_metadatas,
                ids=batch_ids
            )
            
            batch_count = len(batch_docs)
            total_added += batch_count
            logger.info(f"Добавлено в ChromaDB: {total_added}/{len(documents)} записей (пакет {i//batch_size + 1})")
        
        final_count = collection.count()
        logger.info(f"✅ ChromaDB коллекция '{collection_name}' успешно создана с {final_count} записями")
        return final_count
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания ChromaDB коллекции: {e}")
        raise

def main() -> int:
    """Основная функция обработки"""
    try:
        # Путь к JSON-файлу
        json_file_path = 'video_data.json'
        
        # Чтение данных
        data = read_json_file(json_file_path)
        
        # Проверка структуры данных
        if 'videos' not in data or not isinstance(data['videos'], list):
            raise ValueError("Некорректная структура JSON файла: отсутствует ключ 'videos' или он не является списком")
        
        logger.info(f"Загружено {len(data['videos'])} видео для обработки")
        
        # Создание и заполнение базы данных SQLite
        create_and_populate_database(data)
        
        # Создание базы эмбеддингов
        create_embedding_database(data)
        
        logger.info("✅ Все этапы обработки успешно завершены!")
        return 0
        
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка в процессе обработки: {e}")
        print(f"\nОшибка: {str(e)}")
        print("Подробности см. в логах")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)