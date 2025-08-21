Запуск БД: 

```bash
docker compose up -d
```

Просмотр данных/таблицы:

```bash
docker exec -it tick_db psql -U root -d tick -c "\d staging_orders" - структура таблицы STAGING_ORDERS с сырыми данными
docker exec -it tick_db psql -U root -d tick -c "\d active_orders" - структура таблицы ACTIVE_ORDERS с активными (обработанными) данными
docker exec -it tick_db psql -U root -d tick -c "SELECT * FROM staging_orders LIMIT 500;" - 500 строк с сырыми данными
docker exec -it tick_db psql -U root -d tick -c "SELECT * FROM active_orders LIMIT 500;" - 500 строк с обработанынми заявками
```

Запуск скриптов:
```bash
cd bsbp-test
python3 src/load.py (загрузит данные из .csv в таблицу staging_orders)
python3 src/transform.py (обработает данные из таблицы staging_orders и загрузит их в таблицу active_orders; выведет результат запроса с самой высокой ценой покупки и активной заявке с самой низкой ценой продажи по заданному инструменту, заданный инструмент указывается в функции get_best_prices('SiZ4'))
