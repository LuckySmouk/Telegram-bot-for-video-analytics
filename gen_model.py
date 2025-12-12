"""
gen_model.py - Универсальный обработчик запросов на естественном языке
"""

import psycopg2
import re
import json
from datetime import datetime
import chromadb
from chromadb.config import Settings
import ollama
import logging
from typing import Optional, Dict, Any, Union
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('VideoAnalytics')

# Параметры подключения к PostgreSQL
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'video_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

class VideoAnalytics:
    """Система анализа видео данных с поддержкой естественного языка"""
    
    # Карта месяцев для парсинга дат (вспомогательная)
    MONTH_MAP = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
        'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
        'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
        'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
    }

    def __init__(self, chroma_path: str = './chroma_db'):
        """Инициализация системы анализа"""
        # Подключение к PostgreSQL
        try:
            self.conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=int(DB_CONFIG['port']),
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            logger.info(f"✅ Подключено к PostgreSQL: {DB_CONFIG['database']}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise
        
        # Инициализация ChromaDB
        try:
            self.chroma_client = chromadb.PersistentClient(
                path=chroma_path,
                settings=Settings(anonymized_telemetry=False)
            )
            self.collection = self.chroma_client.get_collection(name='video_schema')
            logger.info(f"✅ ChromaDB коллекция загружена: {self.collection.count()} записей")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки ChromaDB: {e}")
            raise

    def __del__(self):
        """Закрытие соединений"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("🔌 Соединение с PostgreSQL закрыто")

    def _normalize_date_in_question(self, question: str) -> str:
        """
        Нормализация дат в вопросе для корректной передачи в SQL
        Преобразует "28 ноября 2025" в "2025-11-28"
        """
        # Паттерн для русских дат
        pattern = r'(\d{1,2})\s+(' + '|'.join(self.MONTH_MAP.keys()) + r')\s+(\d{4})'
        
        def replace_date(match):
            day = int(match.group(1))
            month_name = match.group(2).lower()
            year = int(match.group(3))
            month = self.MONTH_MAP.get(month_name)
            
            if month:
                try:
                    date = datetime(year, month, day)
                    return date.strftime('%Y-%m-%d')
                except ValueError:
                    return match.group(0)
            return match.group(0)
        
        normalized = re.sub(pattern, replace_date, question, flags=re.IGNORECASE)
        return normalized

    def _get_schema_context(self) -> str:
        """Получение контекста схемы БД из ChromaDB"""
        try:
            # Получаем документ схемы
            results = self.collection.get(ids=["schema_v1"])
            if results and results['documents']:
                return results['documents'][0]
            return ""
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить схему из ChromaDB: {e}")
            return ""

    def _generate_sql_from_question(self, question: str) -> Optional[str]:
        """
        Генерация SQL-запроса из вопроса на естественном языке
        Использует LLM (Ollama) для понимания намерения и создания запроса
        """
        # Нормализуем даты в вопросе
        normalized_question = self._normalize_date_in_question(question)
        
        # Получаем контекст схемы
        schema_context = self._get_schema_context()
        
        # Формируем промпт для LLM
        prompt = f"""Ты — эксперт по SQL и PostgreSQL. Твоя задача — сгенерировать ОДИН SQL-запрос на основе вопроса пользователя.

СХЕМА БАЗЫ ДАННЫХ:
{schema_context}

ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ:
- Используй PostgreSQL синтаксис
- Все даты в формате TIMESTAMP, используй DATE() для извлечения даты
- Для диапазонов дат: WHERE DATE(field) BETWEEN 'date1' AND 'date2'
- Для одной даты: WHERE DATE(field) = 'date'
- Запрос ДОЛЖЕН вернуть ОДНО число (используй COUNT, SUM и т.д.)
- НЕ используй подзапросы без необходимости
- Для "включительно" используй BETWEEN (он включает границы)

ВАЖНЫЕ ПАТТЕРНЫ:
1. "Сколько всего видео?" → SELECT COUNT(*) FROM videos
2. "Сколько видео у креатора X с date1 по date2?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND DATE(video_created_at) BETWEEN 'date1' AND 'date2'
3. "Сколько видео набрало больше N просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > N
4. "На сколько просмотров выросли видео за date?" → SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = 'date'
5. "Сколько видео получали просмотры за date?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = 'date' AND delta_views_count > 0

ВОПРОС ПОЛЬЗОВАТЕЛЯ:
{normalized_question}

ИНСТРУКЦИЯ:
Верни ТОЛЬКО SQL-запрос без пояснений, комментариев и markdown. Если не можешь составить запрос - верни "ERROR".

SQL-ЗАПРОС:"""

        try:
            response = ollama.chat(
                model='qwen2.5:7b',  # Более стабильная модель для SQL
                messages=[{'role': 'user', 'content': prompt}],
                options={
                    'temperature': 0.0,  # Детерминированность
                    'num_ctx': 8192,
                    'top_p': 0.1
                }
            )
            
            sql_query = response['message']['content'].strip()
            
            # Очистка от markdown
            sql_query = re.sub(r'```sql\s*', '', sql_query, flags=re.IGNORECASE)
            sql_query = re.sub(r'```\s*$', '', sql_query)
            sql_query = sql_query.strip()
            
            # Проверка на ошибку
            if sql_query.upper() == "ERROR" or not sql_query:
                logger.warning("⚠️ LLM не смогла сгенерировать SQL")
                return None
            
            # Базовая валидация
            if not sql_query.upper().startswith("SELECT"):
                logger.warning(f"⚠️ Некорректный SQL (не начинается с SELECT): {sql_query}")
                return None
            
            # Проверка на опасные операции
            dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', 'GRANT', 'REVOKE']
            sql_upper = sql_query.upper()
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                logger.error(f"🚨 ОПАСНЫЙ SQL-запрос обнаружен: {sql_query}")
                return None
            
            logger.info(f"✅ Сгенерирован SQL: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации SQL через LLM: {e}")
            return None

    def _execute_sql_query(self, sql_query: str) -> Optional[Union[int, float]]:
        """
        Безопасное выполнение SQL-запроса
        Возвращает единственное число или None
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                # Преобразуем в int если возможно
                value = result[0]
                if isinstance(value, (int, float)):
                    return int(value) if isinstance(value, float) and value.is_integer() else value
                return int(value) if value else 0
            
            return 0  # Если результата нет - возвращаем 0
            
        except psycopg2.Error as e:
            logger.error(f"❌ Ошибка выполнения SQL: {e}")
            logger.error(f"   Запрос: {sql_query}")
            return None
        finally:
            if cursor:
                cursor.close()

    def process_question(self, question: str) -> str:
        """
        Основной метод обработки вопроса
        
        Args:
            question: Вопрос на естественном языке (русский)
        
        Returns:
            Строка с числовым ответом или сообщение об ошибке
        """
        logger.info(f"❓ Получен вопрос: {question}")
        
        # Шаг 1: Генерация SQL
        sql_query = self._generate_sql_from_question(question)
        
        if not sql_query:
            return "Извините, не удалось понять ваш вопрос. Попробуйте переформулировать."
        
        # Шаг 2: Выполнение SQL
        result = self._execute_sql_query(sql_query)
        
        if result is None:
            return "Ошибка при выполнении запроса к базе данных."
        
        logger.info(f"✅ Результат: {result}")
        return str(result)

def main():
    """Интерактивный режим для тестирования"""
    print("=" * 70)
    print("🎬 СИСТЕМА АНАЛИТИКИ ВИДЕО ДАННЫХ")
    print("=" * 70)
    print("\nПримеры вопросов:")
    print("  • Сколько всего видео есть в системе?")
    print("  • Сколько видео у креатора с id XXX вышло с 1 по 5 ноября 2025?")
    print("  • Сколько видео набрало больше 100000 просмотров?")
    print("  • На сколько просмотров выросли все видео 28 ноября 2025?")
    print("  • Сколько видео получали новые просмотры 27 ноября 2025?")
    print("\nДля выхода: 'exit'")
    print("-" * 70)
    
    try:
        analytics = VideoAnalytics()
        
        while True:
            try:
                question = input("\n💬 Ваш вопрос: ").strip()
                
                if not question:
                    continue
                
                if question.lower() in ['exit', 'quit', 'выход']:
                    print("\n👋 До свидания!")
                    break
                
                print("🤔 Обрабатываю...")
                answer = analytics.process_question(question)
                print(f"📊 Ответ: {answer}")
                
            except KeyboardInterrupt:
                print("\n\n⚠️ Прервано пользователем")
                break
            except Exception as e:
                logger.exception(f"❌ Ошибка: {e}")
                print(f"❌ Произошла ошибка: {str(e)}")
    
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка при запуске: {e}")
        print(f"\n❌ Не удалось запустить систему: {str(e)}")
        print("Проверьте:")
        print("  1. PostgreSQL запущен и доступен")
        print("  2. База данных создана (запустите json_to_base.py)")
        print("  3. ChromaDB коллекция существует")

if __name__ == "__main__":
    main()