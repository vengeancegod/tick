import os
from dotenv import load_dotenv
import psycopg2
import time

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def get_unprocessed_orders(conn, batch_size=1000):
    with conn.cursor() as c:
        c.execute("SELECT id, instrument, operation, timestamp, order_id, order_type, price, volume FROM staging_orders WHERE processed = FALSE ORDER BY timestamp, id", (batch_size,))
        columns = [desc[0] for desc in c.description]
        return [dict(zip(columns, row)) for row in c.fetchall()]

def handle_new_order(conn, order):

    with conn.cursor() as c:
        c.execute("SELECT order_id FROM active_orders WHERE order_id = %s", (order['order_id'],))
        existing = c.fetchone()
        
        if existing:
            # апдейт существующей заявки
            c.execute("UPDATE active_orders SET remaining_volume = %s, last_updated_timestamp = %s WHERE order_id = %s", (order['volume'], order['timestamp'], order['order_id']))
        else:

            c.execute("INSERT INTO active_orders (order_id, instrument, operation, initial_volume, remaining_volume, price, created_timestamp, last_updated_timestamp, is_active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (
                order['order_id'], order['instrument'], order['operation'], 
                order['volume'], order['volume'], order['price'], 
                order['timestamp'], order['timestamp'], True
            ))

def handle_trade(conn, order):
    #2
    with conn.cursor() as c:
        c.execute("SELECT remaining_volume FROM active_orders WHERE order_id = %s AND is_active = TRUE", (order['order_id'],))
        
        result = c.fetchone()
        if result:
            remaining_volume = result[0]
            new_volume = remaining_volume - order['volume']
            
            if new_volume <= 0:
                c.execute("UPDATE active_orders SET remaining_volume = 0, is_active = FALSE, last_updated_timestamp = %s WHERE order_id = %s", (order['timestamp'], order['order_id']))
            else:
                c.execute("UPDATE active_orders SET remaining_volume = %s, last_updated_timestamp = %s WHERE order_id = %s", (new_volume, order['timestamp'], order['order_id']))

def handle_cancellation(conn, order):
    #0
    with conn.cursor() as c:
        c.execute("UPDATE active_orders SET is_active = FALSE, last_updated_timestamp = %s WHERE order_id = %s AND is_active = TRUE", (order['timestamp'], order['order_id']))

def mark_as_processed(conn, order_id):
    with conn.cursor() as c:
        c.execute("UPDATE staging_orders SET processed = TRUE WHERE id = %s", (order_id,))

def transform_staging_to_active():
    conn = get_db_connection()
    
    try:
        unprocessed_orders = get_unprocessed_orders(conn)
        
        for order in unprocessed_orders:
            try:
                order_type = order['order_type']
                
                if order_type == 1:  # выставление
                    handle_new_order(conn, order)
                elif order_type == 2:  # сделка
                    handle_trade(conn, order)
                elif order_type == 0: # снятие
                    handle_cancellation(conn, order)
                
                mark_as_processed(conn, order['id'])
                conn.commit()
                
            except Exception as e:
                print(f"Ошибка при обработке order_id {order['order_id']}: {e}")
                conn.rollback()
                continue
        
        print(f"Трансформация выполнена")
        
    finally:
        conn.close()

def get_best_prices(instrument):
    conn = get_db_connection()
    
    try:
        with conn.cursor() as c:
            # самая высокая цена покупки
            c.execute("SELECT order_id, price, remaining_volume FROM active_orders WHERE instrument = %s AND operation = 'B' AND is_active = TRUE ORDER BY price DESC, last_updated_timestamp ASC LIMIT 1", (instrument,))
            best_buy = c.fetchone()
            
            # самая низкая цена продажи
            c.execute("SELECT order_id, price, remaining_volume FROM active_orders WHERE instrument = %s AND operation = 'S' AND is_active = TRUE ORDER BY price ASC, last_updated_timestamp ASC LIMIT 1", (instrument,))
            best_sell = c.fetchone()
            return best_buy, best_sell
            
    finally:
        conn.close()

if __name__ == "__main__":
    transform_staging_to_active()
    
    best_buy, best_sell = get_best_prices('SiZ4')
    
    if best_buy:
        print(f"Лучшая покупка: id={best_buy[0]}, цена={best_buy[1]}, объем={best_buy[2]}")
    else:
        print("Нет активных заявок на покупку")
    
    if best_sell:
        print(f"Лучшая продажа: id={best_sell[0]}, цена={best_sell[1]}, объем={best_sell[2]}")
    else:
        print("Нет активных заявок на продажу")