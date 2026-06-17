from airflow.providers.postgres.hooks.postgres import PostgresHook

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS adverse_events (
        id          SERIAL PRIMARY KEY,
        receive_date VARCHAR(8),
        report_id   VARCHAR(50) UNIQUE,  -- UNIQUE garante que o ON CONFLICT funcione
        serious     SMALLINT,
        drugs       TEXT,
        reactions   TEXT,
        ingested_at TIMESTAMP DEFAULT NOW()
    )
"""

_INSERT = """
    INSERT INTO adverse_events (receive_date, report_id, serious, drugs, reactions)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (report_id) DO NOTHING  -- idempotente: rodar 2x não duplica registros
"""


def load_to_postgres(**context):
    data = context['ti'].xcom_pull(task_ids='transform_data')

    if not data:
        return

    # PostgresHook é a abstração do Airflow pra conexões com Postgres.
    # As credenciais (host, porta, user, senha, banco) ficam na conexão 'app_postgres',
    # configurada via variável de ambiente no compose.yaml — não hardcodadas aqui.
    hook = PostgresHook(postgres_conn_id='app_postgres')
    conn = hook.get_conn()  # retorna uma conexão psycopg2 normal
    cursor = conn.cursor()

    try:
        cursor.execute(_CREATE_TABLE)
        rows = [
            (r['receive_date'], r['report_id'], r['serious'], r['drugs'], r['reactions'])
            for r in data
        ]
        cursor.executemany(_INSERT, rows)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
