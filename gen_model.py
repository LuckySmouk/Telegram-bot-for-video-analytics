# gen_model.py

import sqlite3
import re
from datetime import datetime
import chromadb
import ollama
import json

class VideoAnalytics:
    def __init__(self, db_path='video_data.db', chroma_path='./chroma_db'):
        self.db_path = db_path
        self.chroma_path = chroma_path
        self.chroma_client = chromadb.PersistentClient(path=chroma_path)
        self.collection = self.chroma_client.get_collection(name='video_embeddings')

    def query_sqlite(self, query, params=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        conn.close()
        return result

    def parse_date(self, date_str):
        month_map = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }

        parts = date_str.split()
        if len(parts) == 3:
            day = int(parts[0])
            month = month_map.get(parts[1].lower())
            year = int(parts[2])
            if month:
                return datetime(year, month, day)
        return None

    def parse_date_range(self, date_range_str):
        if 'с' in date_range_str and 'по' in date_range_str:
            parts = re.split(r' с | по ', date_range_str)
            if len(parts) == 3:
                start_date = self.parse_date(parts[1])
                end_date = self.parse_date(parts[2])
                if start_date and end_date:
                    return start_date, end_date
        elif 'по' in date_range_str:
            parts = date_range_str.split(' по ')
            if len(parts) == 2:
                date = self.parse_date(parts[1])
                if date:
                    return date, date
        else:
            date = self.parse_date(date_range_str)
            if date:
                return date, date
        return None, None

    def get_video_count(self):
        query = "SELECT COUNT(*) FROM videos"
        result = self.query_sqlite(query)
        return result[0][0]

    def get_videos_by_creator_in_date_range(self, creator_id, start_date, end_date):
        query = """
        SELECT COUNT(*)
        FROM videos
        WHERE creator_id = ? AND video_created_at BETWEEN ? AND ?
        """
        params = (creator_id, start_date.isoformat(), end_date.isoformat())
        result = self.query_sqlite(query, params)
        return result[0][0]

    def get_videos_with_views_more_than(self, views_threshold):
        query = "SELECT COUNT(*) FROM videos WHERE views_count > ?"
        params = (views_threshold,)
        result = self.query_sqlite(query, params)
        return result[0][0]

    def get_total_views_growth_on_date(self, date):
        query = """
        SELECT SUM(delta_views_count)
        FROM video_snapshots
        WHERE created_at LIKE ?
        """
        params = (f"%{date.year}-{date.month:02d}-{date.day:02d}%",)
        result = self.query_sqlite(query, params)
        return result[0][0] or 0

    def get_unique_videos_with_new_views_on_date(self, date):
        query = """
        SELECT COUNT(DISTINCT video_id)
        FROM video_snapshots
        WHERE created_at LIKE ? AND delta_views_count > 0
        """
        params = (f"%{date.year}-{date.month:02d}-{date.day:02d}%",)
        result = self.query_sqlite(query, params)
        return result[0][0]

    def search_in_embeddings(self, query_text, n_results=3):
        results = self.collection.query(
            query_texts=[query_text],
            n_results=n_results
        )
        return results

    def process_question(self, question):
        # Шаг 1: Используем эмбеддинги для поиска релевантной информации
        embedding_results = self.search_in_embeddings(question)
        documents = embedding_results.get('documents', [[]])
        if not documents or not documents[0]:
            context = ""
        else:
            context = "\n".join(str(doc) for doc in documents[0])

        # Шаг 2: Формируем запрос к модели с учётом контекста
        prompt = f"""Контекст:
    {context}

    Вопрос: {question}

    Инструкции:
    1. Проанализируй вопрос и контекст.
    2. Определи, какой метод нужно вызвать для получения ответа.
    3. Извлеки необходимые параметры из вопроса.
    4. Верни ответ в формате JSON с полями:
    - method: название метода
    - params: параметры для метода
    - answer: ответ на вопрос (если можно вычислить сразу)

    Доступные методы:
    1. get_video_count() - получить общее количество видео в системе
    2. get_videos_by_creator_in_date_range(creator_id, start_date, end_date) - получить количество видео у креатора в диапазоне дат
    3. get_videos_with_views_more_than(views_threshold) - получить количество видео с просмотрами больше указанного значения
    4. get_total_views_growth_on_date(date) - получить суммарный рост просмотров за указанную дату
    5. get_unique_videos_with_new_views_on_date(date) - получить количество уникальных видео с новыми просмотрами за указанную дату"""
        try:
            response = ollama.chat(
                model='qwen3-vl:8b-instruct-q8_0',
                messages=[{
                    'role': 'user',
                    'content': prompt,
                    'images': []
                }]
            )

            # Обрабатываем ответ от модели
            message_content = response['message']['content']

            try:
                # Пробуем разобрать как JSON
                answer_data = json.loads(message_content)
                if not isinstance(answer_data, dict):
                    raise ValueError("Answer is not a dictionary")

                method = answer_data.get('method')
                params = answer_data.get('params', {})
                direct_answer = answer_data.get('answer')

                if direct_answer is not None:
                    return str(direct_answer)

                if method == 'get_video_count':
                    return str(self.get_video_count())
                elif method == 'get_videos_by_creator_in_date_range':
                    creator_id = params.get('creator_id')
                    date_range = params.get('date_range')
                    if creator_id and date_range:
                        start_date, end_date = self.parse_date_range(date_range)
                        if start_date and end_date:
                            return str(self.get_videos_by_creator_in_date_range(creator_id, start_date, end_date))
                elif method == 'get_videos_with_views_more_than':
                    views_threshold = params.get('views_threshold')
                    if views_threshold is not None:
                        return str(self.get_videos_with_views_more_than(views_threshold))
                elif method == 'get_total_views_growth_on_date':
                    date_str = params.get('date')
                    if date_str:
                        date = self.parse_date(date_str)
                        if date:
                            return str(self.get_total_views_growth_on_date(date))
                elif method == 'get_unique_videos_with_new_views_on_date':
                    date_str = params.get('date')
                    if date_str:
                        date = self.parse_date(date_str)
                        if date:
                            return str(self.get_unique_videos_with_new_views_on_date(date))

                return "Извините, не удалось определить метод для ответа на вопрос."

            except (json.JSONDecodeError, ValueError) as e:
                # Если ответ не в формате JSON или не словарь, пробуем извлечь число напрямую
                numbers = re.findall(r'\d+', message_content)
                if numbers:
                    return numbers[0]
                return "Извините, не удалось извлечь числовой ответ из ответа модели."

        except Exception as e:
            print(f"Ошибка при обработке ответа: {e}")
            return "Извините, произошла ошибка при обработке вашего вопроса."

def main():
    analytics = VideoAnalytics()

    print("Добро пожаловать в систему анализа видео!")
    print("Вы можете задавать вопросы на русском языке, например:")
    print("- Сколько всего видео есть в системе?")
    print("- Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?")
    print("- Сколько видео набрало больше 100000 просмотров за всё время?")
    print("- На сколько просмотров в сумме выросли все видео 28 ноября 2025?")
    print("- Сколько разных видео получали новые просмотры 27 ноября 2025?")
    print("Для выхода введите 'exit'")

    while True:
        question = input("\nВаш вопрос: ")
        if question.lower() == 'exit':
            break

        answer = analytics.process_question(question)
        print(f"Ответ: {answer}")

if __name__ == "__main__":
    main()
