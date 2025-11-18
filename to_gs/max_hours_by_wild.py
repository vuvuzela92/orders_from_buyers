from gspread_dataframe import set_with_dataframe
from utils.utils_gspread import safe_open_spreadsheet
from utils.utils_sql import create_connection_to_vector_db, get_db_table
from datetime import datetime

def main():
    # === Запрашиваем данные по закупке товаров от поставщиков
    # Соединение
    connection = create_connection_to_vector_db()
    # Запрос закупок
    query_orders = """SELECT ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(sao.created_at))) / 3600) AS hours_passed,
        sao.local_vendor_code
    FROM supplies_and_orders sao
    LEFT JOIN status_assembly_task sat
    ON sao.id = sat.assembly_task_id
    WHERE wb_status = 'waiting'
    GROUP BY local_vendor_code, sao.id
    HAVING EXTRACT(EPOCH FROM (NOW() - MAX(sao.created_at))) / 3600 < 240
    ORDER BY hours_passed DESC; """
    # Выгрузка закупок в датафрейм
    df_max_hours_orders = get_db_table(query_orders, connection)

    #=== Выгружаем данные в гугл-таблицу ===
    table = safe_open_spreadsheet("Тест Расчет закупки")
    sheet_orders = table.worksheet("макс_часы")
    # Выгрузка закупок
    set_with_dataframe(sheet_orders, df_max_hours_orders, resize=True)

    # Выгружаем дату с временем
    formatted_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    max_columns = sheet_orders.col_count
    sheet_orders.update_cell(1, max_columns, formatted_time)
    print("Данные загружены в гугл-таблицу успешно")