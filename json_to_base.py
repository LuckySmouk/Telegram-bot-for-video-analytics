# json_to_base.py

import json
import sqlite3
from datetime import datetime
import chromadb
import ollama

# Чтение JSON-файла
def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Создание и заполнение базы данных SQLite
def create_and_populate_database(data, db_name='video_data.db'):
    # Подключение к базе данных
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Создание таблицы videos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        id TEXT PRIMARY KEY,
        creator_id TEXT,
        video_created_at TEXT,
        views_count INTEGER,
        likes_count INTEGER,
        comments_count INTEGER,
        reports_count INTEGER,
        created_at TEXT,
        updated_at TEXT
    )
    ''')

    # Создание таблицы video_snapshots
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS video_snapshots (
        id TEXT PRIMARY KEY,
        video_id TEXT,
        views_count INTEGER,
        likes_count INTEGER,
        comments_count INTEGER,
        reports_count INTEGER,
        delta_views_count INTEGER,
        delta_likes_count INTEGER,
        delta_comments_count INTEGER,
        delta_reports_count INTEGER,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY (video_id) REFERENCES videos (id)
    )
    ''')

    # Заполнение таблицы videos
    for video in data['videos']:
        cursor.execute('''
        INSERT OR REPLACE INTO videos (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video['id'],
            video['creator_id'],
            video['video_created_at'],
            video['views_count'],
            video['likes_count'],
            video['comments_count'],
            video['reports_count'],
            video.get('created_at', datetime.now().isoformat()),
            video.get('updated_at', datetime.now().isoformat())
        ))

    # Заполнение таблицы video_snapshots
    for video in data['videos']:
        for snapshot in video.get('snapshots', []):
            cursor.execute('''
            INSERT OR REPLACE INTO video_snapshots (
                id, video_id, views_count, likes_count, comments_count, reports_count,
                delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                snapshot['id'],
                video['id'],
                snapshot['views_count'],
                snapshot['likes_count'],
                snapshot['comments_count'],
                snapshot['reports_count'],
                snapshot['delta_views_count'],
                snapshot['delta_likes_count'],
                snapshot['delta_comments_count'],
                snapshot['delta_reports_count'],
                snapshot['created_at'],
                snapshot.get('updated_at', datetime.now().isoformat())
            ))

    # Сохранение изменений и закрытие соединения
    conn.commit()
    conn.close()

# Функция для генерации эмбеддингов с использованием Ollama
def generate_embeddings(texts, model='nomic-embed-text-v2-moe'):
    embeddings = []
    for text in texts:
        response = ollama.embeddings(model=model, prompt=text)
        embeddings.append(response['embedding'])
    return embeddings

# Создание базы эмбеддингов в ChromaDB
def create_embedding_database(data, collection_name='video_embeddings'):
    # Инициализация ChromaDB
    client = chromadb.PersistentClient(path="./chroma_db")

    # Создание коллекции
    collection = client.get_or_create_collection(name=collection_name)

    # Подготовка данных для ChromaDB
    documents = []
    metadatas = []
    ids = []

    for video in data['videos']:
        # Создаем текстовое описание для каждого видео
        text = f"Video ID: {video['id']}, Creator ID: {video['creator_id']}"
        documents.append(text)
        metadatas.append({
            "video_id": video['id'],
            "creator_id": video['creator_id']
        })
        ids.append(video['id'])

    # Генерация эмбеддингов с помощью Ollama
    embeddings = generate_embeddings(documents)

    # Добавление в коллекцию
    collection.add(
        embeddings=embeddings,
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

    return collection

def main():
    # Путь к JSON-файлу
    json_file_path = 'tester.json'  # Замени на реальный путь к файлу

    # Чтение данных
    data = read_json_file(json_file_path)

    # Создание и заполнение базы данных SQLite
    create_and_populate_database(data)

    # Создание базы эмбеддингов
    collection = create_embedding_database(data)
    print(f"Embedding collection created with {collection.count()} entries.")

if __name__ == "__main__":
    main()
