# Airflow Use Case — OpenFDA Adverse Events Pipeline

Pipeline de ingestão horária de dados de adverse events de medicamentos da [OpenFDA API](https://open.fda.gov/apis/drug/event/) para um banco PostgreSQL, construído com Apache Airflow 2.10.

## Arquitetura

```
OpenFDA API (/drug/event.json)
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│                    DAG: openfda_adverse_events             │
│                                                           │
│  extract_data ──► transform_data ──► branch_on_data       │
│                                          │         │      │
│                                    [tem dados] [sem dados]│
│                                          │         │      │
│                                  load_to_postgres  skip   │
│                                          │         │      │
│                                          └────┬────┘      │
│                                              end          │
└───────────────────────────────────────────────────────────┘
        │
        ▼
PostgreSQL (pharma_db.adverse_events)
```

## Infraestrutura

| Serviço | Imagem | Função |
|---|---|---|
| `airflow-webserver` | apache/airflow:2.10.3 | UI na porta 8080 |
| `airflow-scheduler` | apache/airflow:2.10.3 | Orquestra e executa as tasks |
| `postgres` | postgres:15 | Banco de metadados do Airflow |
| `app-postgres` | postgres:15 | Banco de destino dos dados (porta 5433) |

Executor: **LocalExecutor** — paralelismo sem a complexidade do Celery.

## Conceitos do Airflow demonstrados

### 1. Task Dependencies

O fluxo entre tasks é declarado com o operador `>>`, que define a ordem de execução:

```python
extract >> transform >> branch >> [load, skip] >> end
```

O Airflow garante que cada task só executa após a anterior ter concluído com sucesso.

### 2. XComs

XCom (*cross-communication*) é o mecanismo do Airflow para passar dados entre tasks. Cada `PythonOperator` retorna um valor que fica armazenado no banco de metadados e pode ser recuperado por tasks subsequentes:

```python
# extract_data retorna os dados — automaticamente salvo como XCom
return response.json().get('results', [])

# transform_data lê o XCom da task anterior
raw_data = context['ti'].xcom_pull(task_ids='extract_data')
```

### 3. Connections & Hooks

Connections são credenciais e endpoints armazenados no Airflow (via UI, env vars ou secrets backend). Hooks são a camada de abstração para acessar esses sistemas:

```python
# HTTP Hook — usa a connection 'openfda_api'
hook = HttpHook(http_conn_id='openfda_api', method='GET')
response = hook.run(endpoint='/drug/event.json', data={...})

# PostgreSQL Hook — usa a connection 'app_postgres'
hook = PostgresHook(postgres_conn_id='app_postgres')
conn = hook.get_conn()
```

As connections são configuradas via variáveis de ambiente no `compose.yaml`:

```yaml
AIRFLOW_CONN_OPENFDA_API: '{"conn_type": "http", "host": "api.fda.gov", "schema": "https"}'
AIRFLOW_CONN_APP_POSTGRES: '{"conn_type": "postgres", "host": "app-postgres", ...}'
```

### 4. Branching

O `BranchPythonOperator` permite ramificar o fluxo com base em lógica condicional. A função retorna o `task_id` da próxima task a executar — as demais são marcadas como `skipped`:

```python
def branch_on_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform_data')
    return 'load_to_postgres' if data else 'skip'
```

A task `end` usa `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` para executar independentemente de qual branch foi tomado.

## Estrutura do projeto

```
airflow-use-case/
├── compose.yaml              # Infraestrutura completa
├── Dockerfile                # Airflow 2.10.3 + providers
├── requirements.txt          # apache-airflow-providers-http/postgres
├── .env                      # Credenciais (não versionado)
└── dags/
    ├── openfda_pipeline.py   # Definição do DAG
    └── utils/
        ├── extract.py        # Chama OpenFDA via HTTP Hook
        ├── transform.py      # Flatten e normalização dos dados
        └── load.py           # Inserção no PostgreSQL via Hook
```

## Como executar

```bash
# 1. Subir o ambiente
docker compose up --build -d

# 3. Acessar a UI
# http://localhost:8080 (usuário: airflow / senha: airflow)

# 4. Disparar o DAG manualmente com data histórica
docker compose exec airflow-scheduler \
  airflow dags trigger openfda_adverse_events --exec-date 2024-03-15T10:00:00+00:00

# 5. Verificar os dados carregados
docker compose exec app-postgres \
  psql -U appuser -d pharma_db -c "SELECT * FROM adverse_events LIMIT 10;"
```

## Schema da tabela de destino

```sql
CREATE TABLE adverse_events (
    id           SERIAL PRIMARY KEY,
    receive_date VARCHAR(8),           -- ex: 20240315
    report_id    VARCHAR(50) UNIQUE,   -- ID do report na FDA
    serious      SMALLINT,             -- 1 = sério, 2 = fatal
    drugs        TEXT,                 -- nomes dos medicamentos
    reactions    TEXT,                 -- reações adversas (MedDRA)
    ingested_at  TIMESTAMP DEFAULT NOW()
);
```
