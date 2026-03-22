Для запуска достаточно выполнить в корне проекта 
```shell
docker-compose up --build
```

http://localhost:3000/ - фронтенд приложения генерации отчетов (пользователь может загрузить свой отчет из clickhouse после авторизации)

http://localhost:8081/ - фронтенд airflow
login: admin
pass: admin

Для airflow создан DAG, который запускается раз в 15 минут и: 
1. Создаёт таблицу customer_report_mart в ClickHouse (если не существует).
2. (Опционально) генерирует тестовые данные для телеметрии (можно включить через Airflow UI: Admin → Variables → добавить enable_telemetry_generation = true)
3. Собирает данные телеметрии, объединяя их с данными CRM, а затем записывает в clickhouse.
