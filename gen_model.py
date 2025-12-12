import sqlite3
import re
import json
from datetime import datetime, timedelta
import chromadb
from chromadb.config import Settings
import ollama
import logging
from typing import Optional, Tuple, Dict, Any, List, Union

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='video_analytics.log'
)
logger = logging.getLogger('VideoAnalytics')

class VideoAnalytics:
    """Система анализа видео данных с поддержкой естественного языка"""
    
    # Карта месяцев для парсинга дат
    MONTH_MAP = {
        'января': 1, 'январь': 1, 'янв': 1,
        'февраля': 2, 'февраль': 2, 'фев': 2,
        'марта': 3, 'март': 3, 'мар': 3,
        'апреля': 4, 'апрель': 4, 'апр': 4,
        'мая': 5, 'май': 5,
        'июня': 6, 'июнь': 6, 'июн': 6,
        'июля': 7, 'июль': 7, 'июл': 7,
        'августа': 8, 'август': 8, 'авг': 8,
        'сентября': 9, 'сентябрь': 9, 'сен': 9,
        'октября': 10, 'октябрь': 10, 'окт': 10,
        'ноября': 11, 'ноябрь': 11, 'ноя': 11,
        'декабря': 12, 'декабрь': 12, 'дек': 12
    }
    
    # Доступные методы для обработки вопросов
    AVAILABLE_METHODS = {
        "get_video_count": {
            "description": "Получить общее количество видео в системе",
            "params": {}
        },
        "get_videos_by_creator_in_date_range": {
            "description": "Получить количество видео у креатора в диапазоне дат",
            "params": ["creator_id", "date_range"]
        },
        "get_videos_with_views_more_than": {
            "description": "Получить количество видео с просмотрами больше указанного значения",
            "params": ["views_threshold"]
        },
        "get_total_views_growth_on_date": {
            "description": "Получить суммарный рост просмотров за указанную дату",
            "params": ["date"]
        },
        "get_unique_videos_with_new_views_on_date": {
            "description": "Получить количество уникальных видео с новыми просмотрами за указанную дату",
            "params": ["date"]
        }
    }

    def __init__(self, db_path: str = 'video_data.db', chroma_path: str = './chroma_db'):
        """
        Инициализация системы анализа видео
        
        Args:
            db_path: Путь к SQLite базе данных
            chroma_path: Путь к хранилищу ChromaDB
        """
        self.db_path = db_path
        self.chroma_path = chroma_path
        
        # Инициализация ChromaDB клиента
        self.chroma_client = chromadb.PersistentClient(
            path=chroma_path,
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Безопасное получение коллекции
        try:
            self.collection = self.chroma_client.get_collection(name='video_embeddings')
            logger.info(f"Коллекция ChromaDB успешно загружена: {self.collection.count()} записей")
        except ValueError as e:
            logger.error(f"Ошибка загрузки коллекции 'video_embeddings': {e}")
            raise RuntimeError("Коллекция 'video_embeddings' не найдена в ChromaDB. "
                              "Убедитесь, что запустили json_to_base.py для создания коллекции.")
        
        # Установка соединения с SQLite
        self.conn = sqlite3.connect(self.db_path)
        logger.info(f"Соединение с БД установлено: {db_path}")

    def __del__(self):
        """Закрытие соединений при удалении объекта"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Соединение с БД закрыто")

    def _execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """
        Безопасное выполнение SQL-запроса
        
        Args:
            query: SQL-запрос
            params: Параметры для запроса
        
        Returns:
            Результат запроса
        """
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            return result
        except sqlite3.Error as e:
            logger.error(f"Ошибка выполнения SQL-запроса: {e}\nQuery: {query}\nParams: {params}")
            raise
        finally:
            cursor.close()

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Парсинг даты из текстового представления
        
        Поддерживаемые форматы:
        - "28 ноября 2025"
        - "28 ноя 2025"
        - "28.11.2025"
        - "2025-11-28"
        
        Args:
            date_str: Строка с датой
        
        Returns:
            Объект datetime или None при ошибке
        """
        date_str = date_str.strip().lower()
        
        # Прямое преобразование ISO формата
        try:
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            pass
        
        # Обработка формата "28 ноября 2025"
        parts = re.split(r'[.,\s]+', date_str)
        if len(parts) >= 3:
            # Определение позиции года
            year_index = -1
            for i, part in enumerate(parts):
                if re.match(r'\d{4}', part):
                    year_index = i
                    break
            
            if year_index != -1 and year_index >= 2:
                day = parts[year_index - 2]
                month_name = parts[year_index - 1]
                year = parts[year_index]
                
                try:
                    day = int(day)
                    year = int(year)
                    month = self.MONTH_MAP.get(month_name.strip('.'))
                    
                    if month and 1 <= day <= 31 and 1970 <= year <= 2100:
                        return datetime(year, month, day)
                except (ValueError, TypeError):
                    pass
        
        # Обработка формата "28.11.2025"
        try:
            return datetime.strptime(date_str, '%d.%m.%Y')
        except ValueError:
            pass
        
        logger.warning(f"Не удалось распарсить дату: {date_str}")
        return None

    def parse_date_range(self, date_range_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Парсинг диапазона дат
        
        Поддерживаемые форматы:
        - "с 1 ноября 2025 по 5 ноября 2025"
        - "1-5 ноября 2025"
        - "по 5 ноября 2025"
        - "5 ноября 2025"
        
        Args:
            date_range_str: Строка с диапазоном дат
        
        Returns:
            Кортеж (start_date, end_date) или (None, None) при ошибке
        """
        date_range_str = date_range_str.lower().strip()
        
        # Формат "1-5 ноября 2025"
        range_match = re.match(
            r'(\d{1,2})\s*[-–]\s*(\d{1,2})\s+(\w+)\s+(\d{4})',
            date_range_str,
            re.IGNORECASE
        )
        if range_match:
            start_day = int(range_match.group(1))
            end_day = int(range_match.group(2))
            month_name = range_match.group(3).strip('.')
            year = int(range_match.group(4))
            
            month = self.MONTH_MAP.get(month_name)
            if month:
                try:
                    start_date = datetime(year, month, start_day)
                    end_date = datetime(year, month, end_day)
                    return start_date, end_date
                except ValueError as e:
                    logger.error(f"Ошибка при создании даты: {e}")
        
        # Формат "с 1 ноября 2025 по 5 ноября 2025"
        if 'с' in date_range_str and 'по' in date_range_str:
            parts = re.split(r'\s+с\s+|\s+по\s+', date_range_str)
            if len(parts) == 3:
                start_date = self.parse_date(parts[1].strip())
                end_date = self.parse_date(parts[2].strip())
                if start_date and end_date:
                    return start_date, end_date
        
        # Формат "по 5 ноября 2025" или "до 5 ноября 2025"
        if re.search(r'по|до', date_range_str):
            date_match = re.search(r'(?:по|до)\s+(.+)', date_range_str)
            if date_match:
                date_str = date_match.group(1).strip()
                date = self.parse_date(date_str)
                if date:
                    # Если указана только конечная дата, считаем начальной датой неделю назад
                    start_date = date - timedelta(days=7)
                    return start_date, date
        
        # Одиночная дата
        single_date = self.parse_date(date_range_str)
        if single_date:
            return single_date, single_date
        
        logger.warning(f"Не удалось распарсить диапазон дат: {date_range_str}")
        return None, None

    def get_video_count(self) -> int:
        """Получить общее количество видео в системе"""
        query = "SELECT COUNT(*) FROM videos"
        result = self._execute_query(query)
        return result[0][0] if result else 0

    def get_videos_by_creator_in_date_range(
        self, 
        creator_id: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> int:
        """
        Получить количество видео у креатора в диапазоне дат
        
        Args:
            creator_id: ID креатора
            start_date: Начальная дата
            end_date: Конечная дата
        
        Returns:
            Количество видео
        """
        query = """
        SELECT COUNT(*) 
        FROM videos 
        WHERE creator_id = ? 
        AND DATE(video_created_at) BETWEEN DATE(?) AND DATE(?)
        """
        params = (creator_id, start_date.isoformat(), end_date.isoformat())
        result = self._execute_query(query, params)
        return result[0][0] if result else 0

    def get_videos_with_views_more_than(self, views_threshold: int) -> int:
        """
        Получить количество видео с просмотрами больше порога
        
        Args:
            views_threshold: Пороговое значение просмотров
        
        Returns:
            Количество видео
        """
        query = "SELECT COUNT(*) FROM videos WHERE views_count > ?"
        params = (views_threshold,)
        result = self._execute_query(query, params)
        return result[0][0] if result else 0

    def get_total_views_growth_on_date(self, date: datetime) -> int:
        """
        Получить суммарный рост просмотров за указанную дату
        
        Args:
            date: Дата для анализа
        
        Returns:
            Суммарный рост просмотров
        """
        query = """
        SELECT COALESCE(SUM(delta_views_count), 0)
        FROM video_snapshots
        WHERE DATE(created_at) = DATE(?)
        """
        params = (date.isoformat(),)
        result = self._execute_query(query, params)
        return result[0][0] if result else 0

    def get_unique_videos_with_new_views_on_date(self, date: datetime) -> int:
        """
        Получить количество уникальных видео с новыми просмотрами за дату
        
        Args:
            date: Дата для анализа
        
        Returns:
            Количество уникальных видео
        """
        query = """
        SELECT COUNT(DISTINCT video_id)
        FROM video_snapshots
        WHERE DATE(created_at) = DATE(?) 
        AND delta_views_count > 0
        """
        params = (date.isoformat(),)
        result = self._execute_query(query, params)
        return result[0][0] if result else 0

    def search_in_embeddings(self, query_text: str, n_results: int = 5) -> Dict[str, Any]:
        """
        Поиск релевантных документов в ChromaDB
        
        Args:
            query_text: Текст запроса
            n_results: Количество результатов
        
        Returns:
            Результаты поиска в формате словаря
        """
        try:
            # Ограничиваем количество результатов реальным количеством в коллекции
            max_results = min(n_results, self.collection.count())
            if max_results == 0:
                logger.warning("Коллекция ChromaDB пуста")
                return {'documents': [[]], 'metadatas': [[]]}
            
            results = self.collection.query(
                query_texts=[query_text],
                n_results=max_results,
                include=['documents', 'metadatas']
            )
            
            # Преобразуем QueryResult в словарь для совместимости
            result_dict = {
                'documents': results['documents'],
                'metadatas': results['metadatas'],
                'distances': results.get('distances', []),
                'ids': results.get('ids', [])
            }
            
            logger.debug(f"Найдено {len(result_dict['documents'][0])} релевантных документов")
            return result_dict
        except Exception as e:
            logger.error(f"Ошибка при поиске в ChromaDB: {e}")
            return {'documents': [[]], 'metadatas': [[]]}

    def _build_context(self, question: str) -> str:
        """
        Формирование контекста для языковой модели на основе эмбеддингов и структуры данных
        
        Args:
            question: Вопрос пользователя
        
        Returns:
            Строка с контекстом
        """
        # Поиск релевантных документов
        embedding_results = self.search_in_embeddings(question)
        
        # Извлечение полезного контента из результатов
        context_items = []
        
        # Добавляем информацию о доступных методах
        methods_info = "ДОСТУПНЫЕ МЕТОДЫ ДЛЯ ОТВЕТА НА ВОПРОС:\n"
        for method_name, method_info in self.AVAILABLE_METHODS.items():
            methods_info += f"- {method_name}: {method_info['description']}\n"
            if method_info['params']:
                methods_info += f"  Параметры: {', '.join(method_info['params'])}\n"
        context_items.append(methods_info)
        
        # Добавляем примеры вопросов и ответов
        examples = """
ПРИМЕРЫ ВОПРОСОВ И ОТВЕТОВ:
- "Сколько всего видео есть в системе?" -> метод: get_video_count
- "Сколько видео у креатора с id abc123 вышло с 1 по 5 ноября 2025?" -> метод: get_videos_by_creator_in_date_range, параметры: creator_id="abc123", date_range="1-5 ноября 2025"
- "Сколько видео набрало больше 100000 просмотров?" -> метод: get_videos_with_views_more_than, параметр: views_threshold=100000
- "На сколько просмотров в сумме выросли все видео 28 ноября 2025?" -> метод: get_total_views_growth_on_date, параметр: date="28 ноября 2025"
- "Сколько разных видео получали новые просмотры 27 ноября 2025?" -> метод: get_unique_videos_with_new_views_on_date, параметр: date="27 ноября 2025"
"""
        context_items.append(examples)
        
        # Добавляем релевантные документы из базы знаний
        documents = embedding_results.get('documents', [[]])[0]
        if documents:
            knowledge = "\nРЕЛЕВАНТНАЯ ИНФОРМАЦИЯ ИЗ БАЗЫ ЗНАНИЙ:\n"
            for i, doc in enumerate(documents[:3], 1):  # Берём не более 3 документов
                knowledge += f"{i}. {doc}\n"
            context_items.append(knowledge)
        
        return "\n\n".join(context_items)

    def _generate_prompt(self, question: str, context: str) -> str:
        """
        Генерация промпта для языковой модели
        
        Args:
            question: Вопрос пользователя
            context: Контекст для модели
        
        Returns:
            Сформированный промпт
        """
        return f"""Ты — система аналитики видео данных. Твоя задача — точно определить, какой метод нужно вызвать для ответа на вопрос пользователя, и извлечь все необходимые параметры.

КОНТЕКСТ:
{context}

ВОПРОС ПОЛЬЗОВАТЕЛЯ:
{question}

ИНСТРУКЦИИ:
1. ВНИМАТЕЛЬНО проанализируй вопрос и контекст.
2. Извлеки ВСЕ необходимые параметры из вопроса. Для дат используй ФОРМАТ "ДД месяц ГГГГ" (например, "28 ноября 2025").
3. Если в вопросе не хватает параметров — сделай обоснованное предположение на основе контекста или используй значения по умолчанию.
4. Верни ответ ТОЛЬКО в формате JSON со следующими полями:
   - "method": название метода (строка)
   - "params": объект с параметрами (пустой объект, если параметры не нужны)
   - "explanation": краткое объяснение выбора (необязательно, но желательно)

ВАЖНО:
- Для дат используй ИСХОДНОЕ текстовое представление из вопроса, не преобразовывай в ISO формат.
- Если не можешь определить метод с уверенностью 95% — верни "method": "unknown".
- ОТВЕТ ДОЛЖЕН БЫТЬ ТОЛЬКО В ФОРМАТЕ JSON, без дополнительного текста.
"""

    def _validate_model_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Валидация и нормализация ответа модели
        
        Args:
            response: Ответ модели в формате JSON
        
        Returns:
            Валидированный ответ
        """
        validated = {
            'method': str(response.get('method', '')).strip(),
            'params': {},
            'explanation': str(response.get('explanation', '')).strip()
        }
        
        # Валидация метода
        if validated['method'] not in self.AVAILABLE_METHODS and validated['method'] != 'unknown':
            logger.warning(f"Недопустимый метод: {validated['method']}")
            validated['method'] = 'unknown'
        
        # Валидация параметров
        raw_params = response.get('params', {})
        if isinstance(raw_params, dict):
            for key, value in raw_params.items():
                if isinstance(value, str):
                    validated['params'][key] = value.strip()
                else:
                    validated['params'][key] = value
        
        return validated

    def process_question(self, question: str) -> str:
        """
        Основной метод обработки вопроса пользователя
        
        Args:
            question: Вопрос на русском языке
        
        Returns:
            Числовой ответ или сообщение об ошибке
        """
        logger.info(f"Получен вопрос: {question}")
        
        try:
            # Шаг 1: Формирование контекста
            context = self._build_context(question)
            
            # Шаг 2: Генерация промпта
            prompt = self._generate_prompt(question, context)
            logger.debug(f"Сформирован промпт: {prompt[:200]}...")
            
            # Шаг 3: Запрос к языковой модели
            try:
                response = ollama.chat(
                    model='qwen3-vl:8b-instruct-q8_0',
                    messages=[{'role': 'user', 'content': prompt}],
                    options={'temperature': 0.1, 'num_ctx': 4096}
                )
                model_response = response['message']['content'].strip()
                logger.debug(f"Ответ модели: {model_response}")
            except Exception as e:
                logger.error(f"Ошибка при обращении к Ollama: {e}")
                return "Извините, сейчас не могу обработать запрос. Попробуйте позже."
            
            # Шаг 4: Парсинг и валидация ответа
            try:
                # Очистка ответа от возможного Markdown-оформления
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', model_response)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_str = model_response
                
                # Очистка от лишних символов в начале и конце
                json_str = re.sub(r'^[^{]*', '', json_str)
                json_str = re.sub(r'[^}]*$', '', json_str)
                
                response_data = json.loads(json_str)
                validated = self._validate_model_response(response_data)
                logger.info(f"Валидированный ответ: {validated}")
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Ошибка парсинга JSON: {e}\nОтвет модели: {model_response}")
                return "Не удалось понять ваш вопрос. Пожалуйста, переформулируйте его."
            
            # Шаг 5: Выполнение метода
            if validated['method'] == 'unknown':
                return "Не удалось определить, как ответить на ваш вопрос. Пожалуйста, задайте его более конкретно."
            
            method_name = validated['method']
            params = validated['params']
            
            try:
                if method_name == 'get_video_count':
                    result = self.get_video_count()
                
                elif method_name == 'get_videos_by_creator_in_date_range':
                    creator_id = params.get('creator_id', '').strip()
                    date_range = params.get('date_range', '').strip()
                    
                    if not creator_id or not date_range:
                        return "Не хватает параметров: требуется ID креатора и диапазон дат."
                    
                    start_date, end_date = self.parse_date_range(date_range)
                    if not start_date or not end_date:
                        return f"Не удалось распарсить диапазон дат: '{date_range}'"
                    
                    result = self.get_videos_by_creator_in_date_range(
                        creator_id, start_date, end_date
                    )
                
                elif method_name == 'get_videos_with_views_more_than':
                    views_threshold = params.get('views_threshold')
                    if views_threshold is None:
                        return "Не указан порог просмотров."
                    
                    try:
                        threshold = int(views_threshold)
                        if threshold < 0:
                            return "Порог просмотров не может быть отрицательным."
                        result = self.get_videos_with_views_more_than(threshold)
                    except (TypeError, ValueError):
                        return f"Некорректное значение порога просмотров: '{views_threshold}'"
                
                elif method_name == 'get_total_views_growth_on_date':
                    date_str = params.get('date', '').strip()
                    if not date_str:
                        return "Не указана дата."
                    
                    date = self.parse_date(date_str)
                    if not date:
                        return f"Не удалось распарсить дату: '{date_str}'"
                    
                    result = self.get_total_views_growth_on_date(date)
                
                elif method_name == 'get_unique_videos_with_new_views_on_date':
                    date_str = params.get('date', '').strip()
                    if not date_str:
                        return "Не указана дата."
                    
                    date = self.parse_date(date_str)
                    if not date:
                        return f"Не удалось распарсить дату: '{date_str}'"
                    
                    result = self.get_unique_videos_with_new_views_on_date(date)
                
                else:
                    return "Неизвестный метод для обработки вопроса."
                
                logger.info(f"Результат для вопроса '{question}': {result}")
                return str(result)
            
            except Exception as e:
                logger.exception(f"Ошибка при выполнении метода {method_name}: {e}")
                return f"Ошибка при получении данных: {str(e)}"
        
        except Exception as e:
            logger.exception(f"Критическая ошибка при обработке вопроса: {e}")
            return "Произошла внутренняя ошибка. Пожалуйста, попробуйте позже."

def main():
    """Основная функция для интерактивного режима"""
    print("=" * 60)
    print("СИСТЕМА АНАЛИТИКИ ВИДЕО ДАННЫХ")
    print("=" * 60)
    print("Задавайте вопросы на русском языке, например:")
    print("- Сколько всего видео есть в системе?")
    print("- Сколько видео у креатора с id abc123 вышло с 1 по 5 ноября 2025?")
    print("- Сколько видео набрало больше 100000 просмотров?")
    print("- На сколько просмотров в сумме выросли все видео 28 ноября 2025?")
    print("- Сколько разных видео получали новые просмотры 27 ноября 2025?")
    print("\nДля выхода введите 'exit' или нажмите Ctrl+C")
    print("-" * 60)
    
    try:
        analytics = VideoAnalytics()
        
        while True:
            try:
                question = input("\n❓ Ваш вопрос: ").strip()
                if not question:
                    continue
                
                if question.lower() in ['exit', 'выход', 'quit']:
                    print("\nСпасибо за использование системы аналитики!")
                    break
                
                print("🤔 Анализирую вопрос...")
                answer = analytics.process_question(question)
                print(f"\n✅ Ответ: {answer}")
            
            except KeyboardInterrupt:
                print("\n\nПринудительное завершение...")
                break
    
    except Exception as e:
        logger.exception(f"Критическая ошибка в основном цикле: {e}")
        print(f"Ошибка при запуске системы: {str(e)}")
        print("Проверьте логи для получения подробной информации.")

if __name__ == "__main__":
    main()