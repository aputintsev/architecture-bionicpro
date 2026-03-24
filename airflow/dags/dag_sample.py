"""
BionicPRO ETL DAG

Пайплайн извлекает данные из двух источников:
  - CRM PostgreSQL (crm.customers)
  - Telemetry PostgreSQL (telemetry.telemetry_data)

Строит витрину данных customer_report_mart в ClickHouse
для сервиса отчётов.

Расписание: каждые 15 минут.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from collections import Counter
import random
import psycopg2
import psycopg2.extras
import clickhouse_connect

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
}

CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123

CRM_DB = {
    'host': 'crm-postgres',
    'port': 5432,
    'dbname': 'crm',
    'user': 'crm_user',
    'password': 'crm_password',
}

TELEMETRY_DB = {
    'host': 'telemetry-postgres',
    'port': 5432,
    'dbname': 'telemetry',
    'user': 'telemetry_user',
    'password': 'telemetry_password',
}


CUSTOMERS = [
    (648821,  'PROS-001'),
    (6488214, 'PROS-002'),
    (6488211, 'PROS-003'),
]
MOVEMENT_TYPES = ['grip', 'open', 'pinch']


# ──────────────────────────────────────────────────────────────
# Шаг 1. Создание витрины в ClickHouse
# ──────────────────────────────────────────────────────────────

def create_mart_table():
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

    # Витрина: агрегированные данные по клиентам для сервиса отчётов.
    # ORDER BY (customer_id, prosthetic_id, report_date) обеспечивает
    # быстрый доступ к данным конкретного пользователя.
    client.command("""
        CREATE TABLE IF NOT EXISTS customer_report_mart (
            customer_id          UInt64,
            prosthetic_id        String,
            report_date          Date,
            customer_name        String,
            customer_email       String,
            total_events         UInt64,
            avg_response_time_ms Float32,
            min_response_time_ms UInt32,
            max_response_time_ms UInt32,
            avg_battery_level    Float32,
            avg_signal_quality   Float32,
            anomaly_count        UInt64,
            most_common_movement String
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (customer_id, prosthetic_id, report_date)
    """)


# ──────────────────────────────────────────────────────────────
# Шаг 2 (опционально). Генерация телеметрии в PostgreSQL
#   Имитация IoT-потока для разработки и тестирования.
#   Управляется через Airflow Variable: enable_telemetry_generation
# ──────────────────────────────────────────────────────────────

def generate_telemetry(**context):
    if Variable.get("enable_telemetry_generation", default_var="true").lower() != "true":
        print("Генерация телеметрии отключена (Airflow Variable: enable_telemetry_generation=false)")
        return

    interval_start = context['data_interval_start']
    interval_end = context['data_interval_end']

    # Если интервал нулевой (ручной запуск), используем окно в 15 минут до текущего момента
    if interval_start == interval_end:
        interval_end = datetime.utcnow()
        interval_start = interval_end - timedelta(minutes=15)

    interval_seconds = (interval_end - interval_start).total_seconds()

    with psycopg2.connect(**TELEMETRY_DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(event_id) FROM telemetry_data")
            max_id = cur.fetchone()[0] or 0

            rows = []
            # Гарантируем хотя бы по одному событию на каждого клиента
            customers_shuffled = CUSTOMERS.copy()
            random.shuffle(customers_shuffled)
            picks = customers_shuffled + [random.choice(CUSTOMERS) for _ in range(10 - len(CUSTOMERS))]

            for i, (customer_id, prosthetic_id) in enumerate(picks):
                # Равномерно распределяем события внутри интервала
                offset = timedelta(seconds=(i / 10) * interval_seconds)
                event_time = interval_start + offset

                rows.append((
                    max_id + i + 1,
                    prosthetic_id,
                    customer_id,
                    event_time,
                    random.randint(30, 250),  # response_time_ms
                    random.randint(10, 100),  # battery_level
                    random.randint(50, 100),  # signal_quality
                    random.choice(MOVEMENT_TYPES),
                    1 if random.random() < 0.1 else 0,  # is_anomaly ~10%
                ))

            cur.executemany("""
                INSERT INTO telemetry_data
                    (event_id, prosthetic_id, customer_id, event_timestamp,
                     response_time_ms, battery_level, signal_quality, movement_type, is_anomaly)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, rows)
        conn.commit()
    print(f'Сгенерировано 10 новых событий телеметрии, event_id: {max_id + 1}–{max_id + 10}')


# ──────────────────────────────────────────────────────────────
# Шаг 3. Построение витрины данных
# ──────────────────────────────────────────────────────────────

def build_mart(**context):
    """
    Извлекаем данные из CRM и Telemetry PostgreSQL,
    агрегируем по клиентам только за прошедший интервал
    и записываем в ClickHouse.
    """
    interval_start = context['data_interval_start']
    interval_end = context['data_interval_end']
    print(f'Обрабатываем телеметрию за период: {interval_start} → {interval_end}')

    # Читаем данные клиентов из CRM
    with psycopg2.connect(**CRM_DB) as crm_conn:
        with crm_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT customer_id, name, email, prosthetic_id FROM customers")
            customers = {row['customer_id']: dict(row) for row in cur.fetchall()}

    # Читаем только телеметрию за текущий интервал
    with psycopg2.connect(**TELEMETRY_DB) as tel_conn:
        with tel_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT customer_id, prosthetic_id, response_time_ms,
                       battery_level, signal_quality, movement_type, is_anomaly
                FROM telemetry_data
                WHERE event_timestamp >= %(start)s AND event_timestamp < %(end)s
            """, {'start': interval_start, 'end': interval_end})
            telemetry_rows = cur.fetchall()

    if not telemetry_rows:
        print('Новых событий телеметрии за интервал нет, пропускаем')
        return

    # Агрегируем телеметрию по (customer_id, prosthetic_id)
    groups = {}
    for row in telemetry_rows:
        key = (row['customer_id'], row['prosthetic_id'])
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(row))

    # Формируем строки для витрины
    report_date = interval_start.date()
    mart_rows = []
    for (customer_id, prosthetic_id), events in groups.items():
        customer = customers.get(customer_id, {})
        response_times = [e['response_time_ms'] for e in events]
        most_common = Counter(e['movement_type'] for e in events).most_common(1)[0][0]

        mart_rows.append((
            customer_id,
            prosthetic_id,
            report_date,
            customer.get('name', ''),
            customer.get('email', ''),
            len(events),
            sum(response_times) / len(response_times),
            min(response_times),
            max(response_times),
            sum(e['battery_level'] for e in events) / len(events),
            sum(e['signal_quality'] for e in events) / len(events),
            sum(1 for e in events if e['is_anomaly'] == 1),
            most_common,
        ))

    # Удаляем старые данные за сегодня перед вставкой, чтобы избежать дублей
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    client.command(f"ALTER TABLE customer_report_mart DELETE WHERE report_date = '{report_date}'")

    client.insert(
        'customer_report_mart',
        mart_rows,
        column_names=[
            'customer_id', 'prosthetic_id', 'report_date',
            'customer_name', 'customer_email',
            'total_events', 'avg_response_time_ms',
            'min_response_time_ms', 'max_response_time_ms',
            'avg_battery_level', 'avg_signal_quality',
            'anomaly_count', 'most_common_movement',
        ],
    )


# ──────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────

with DAG(
    'bionic_pro_etl_dag',
    default_args=default_args,
    description='ETL: CRM + Telemetry PostgreSQL → ClickHouse витрина для сервиса отчётов',
    schedule_interval='*/15 * * * *',  # каждые 15 минут
    catchup=False,
) as dag:

    task_create_mart = PythonOperator(
        task_id='create_mart_table',
        python_callable=create_mart_table,
    )

    task_generate_telemetry = PythonOperator(
        task_id='generate_telemetry',
        python_callable=generate_telemetry,
    )

    task_build_mart = PythonOperator(
        task_id='build_mart',
        python_callable=build_mart,
    )

    task_create_mart >> task_generate_telemetry >> task_build_mart
