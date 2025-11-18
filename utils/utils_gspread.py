import gspread
import logging
import os
from time import time
import sys
# Универсально определяем путь к текущей папке
if '__file__' in globals():
    base_path = os.path.dirname(os.path.abspath(__file__))
else:
    base_path = os.getcwd()  # для Jupyter Notebook

# Добавляем родительскую директорию в sys.path
sys.path.append(os.path.abspath(os.path.join(base_path, '..')))

def safe_open_spreadsheet(title, retries=5, delay=5):
    """
    Пытается открыть таблицу с повторными попытками при APIError 503.
    """
    gc = gspread.service_account(filename=os.path.join(os.path.dirname(__file__), 'creds.json'))
    
    for attempt in range(1, retries + 1):
        logging.info(f"[Попытка {attempt}] открыть доступ к таблице '{title}'")
        
        try:
            spreadsheet = gc.open(title)
            logging.info(f"✅ Таблица '{title}' успешно открыта")
            return spreadsheet
            
        except gspread.exceptions.APIError as e:
            error_code = e.response.status_code if hasattr(e, 'response') else None
            logging.info(f"⚠️ [Попытка {attempt}/{retries}] APIError {error_code}: {e}")
            
            if error_code == 503:
                if attempt < retries:
                    logging.info(f"⏳ Ожидание {delay} секунд перед повторной попыткой...")
                    time.sleep(delay)
                    # Увеличиваем задержку для следующей попытки (exponential backoff)
                    delay *= 2
                else:
                    logging.error("❌ Все попытки исчерпаны")
                    raise
            else:
                # Другие ошибки API (403, 404 и т.д.) - не повторяем
                raise
                
        except gspread.SpreadsheetNotFound:
            logging.info(f"❌ Таблица '{title}' не найдена")
            raise
            
        except Exception as e:
            logging.error(f"⚠️ [Попытка {attempt}/{retries}] Неожиданная ошибка: {e}")
            if attempt < retries:
                logging.error(f"⏳ Ожидание {delay} секунд...")
                time.sleep(delay)
                delay *= 2
            else:
                raise RuntimeError(f"Не удалось открыть таблицу '{title}' после {retries} попыток.")