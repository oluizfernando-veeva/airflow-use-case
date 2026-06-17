# Airflow Use Case — OpenFDA Adverse Events Pipeline

Pipeline que coleta relatórios públicos de efeitos adversos de medicamentos da FDA (agência regulatória americana) e salva num banco PostgreSQL.

## O que são esses dados

Quando um paciente ou médico nos EUA reporta que um medicamento causou um problema de saúde, esse relato vai pra FDA e vira um **adverse event report**. É um banco de dados público chamado FAERS (FDA Adverse Event Reporting System).

Exemplo do que fica salvo após uma execução:

```
report_id | receive_date | serious | drugs         | reactions
----------+--------------+---------+---------------+----------------------
23634463  | 20240315     |    1    | CIPROFLOXACIN | Dizziness postural
23634465  | 20240315     |    1    | LYNPARZA      | Death
23634467  | 20240315     |    1    | SERTRALINE    | Loss of libido
```

- `serious = 1` → caso grave, `serious = 2` → morte
- `drugs` → medicamentos que o paciente usava
- `reactions` → reações que o paciente teve (terminologia MedDRA)

Com esse banco populado dá pra responder perguntas como: quais medicamentos têm mais relatos de morte? A sertralina causou mais efeitos em 2023 ou 2024?

## O que o Airflow faz aqui

O Airflow não processa dado nenhum — ele é o **orquestrador**. O papel dele é:

- Rodar o pipeline todo hora automaticamente (`schedule='@hourly'` em `openfda_pipeline.py`)
- Garantir que as etapas rodam na ordem certa
- Fazer retry se a API falhar (1 retry com 5 min de espera, configurado em `default_args`)
- Mostrar na UI o status de cada etapa, com logs e histórico

## O pipeline em 4 etapas

```
extract_data → transform_data → branch_on_data → load_to_postgres
                                              → skip (se não tiver dados)
```

### 1. Extract — `dags/utils/extract.py`

Chama a API pública `api.fda.gov/drug/event.json` filtrando pela data do run e retorna até 10 registros brutos em JSON.

A data usada é sempre a `data_interval_start` — o início do intervalo de tempo que aquele run representa. É isso que permite reprocessar datas históricas: cada run tem sua própria janela de tempo.

Se a API retornar 404 (nenhum dado pra aquela data), retorna lista vazia.

### 2. Transform — `dags/utils/transform.py`

A API devolve JSON aninhado e cheio de campos que não usamos. O transform pega isso:

```json
{
  "safetyreportid": "23954399",
  "serious": "1",
  "receivedate": "20240610",
  "safetyreportversion": "3",
  "primarysourcecountry": "JP",
  "companynumb": "JP-SA-2024SA164868",
  "patient": {
    "patientonsetage": "62",
    "patientsex": "2",
    "reaction": [
      { "reactionmeddrapt": "Cerebral infarction", "reactionoutcome": "6" },
      { "reactionmeddrapt": "White blood cell count decreased", "reactionoutcome": "6" }
    ],
    "drug": [
      { "medicinalproduct": "SARCLISA", "drugdosageform": "Concentrate for solution for infusion", ... }
    ]
  }
}
```

E transforma nisso:

```
report_id | receive_date | serious | drugs   | reactions
----------+--------------+---------+---------+-----------------------------------------------------
23954399  | 20240610     |    1    | SARCLISA| Cerebral infarction, White blood cell count decreased
```

Descarta tudo que não importa e achata as listas de medicamentos e reações em strings separadas por vírgula.

### 3. Branch — `dags/openfda_pipeline.py`

O `if/else` do pipeline. Olha o que o transform retornou e decide o próximo passo:

- **Tem dados** → vai pra `load_to_postgres`
- **Lista vazia** → vai pra `skip` (não tem nada pra inserir)

Na UI você vê isso visualmente: quando dá skip, o quadrado `load_to_postgres` fica cinza e o `skip` fica verde.

### 4. Load — `dags/utils/load.py`

Cria a tabela `adverse_events` se não existir e insere as linhas. O `ON CONFLICT (report_id) DO NOTHING` garante idempotência — rodar o mesmo pipeline duas vezes na mesma data não duplica registros.

## Como os dados trafegam entre as etapas (XCom)

As etapas são funções Python isoladas. Para passar dados entre elas, o Airflow usa **XCom**: o valor que uma função retorna fica salvo no banco de metadados do Airflow, e a próxima etapa busca de lá com `xcom_pull`.

```python
# extract retorna os dados → salvos automaticamente como XCom
return response.json().get('results', [])

# transform busca o que extract retornou
raw_data = context['ti'].xcom_pull(task_ids='extract_data')
```

## Onde os dados ficam

São dois bancos PostgreSQL separados rodando via Docker:

```
├── postgres:5432        ← banco interno do Airflow (metadados, XComs, logs)
└── app-postgres:5433    ← banco da aplicação
    └── pharma_db
        └── adverse_events  ← aqui ficam os dados da FDA
```

## Como rodar

```bash
# 1. Copiar o .env e ajustar o AIRFLOW_UID pro seu usuário
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env   # evita erros de permissão no diretório logs/

# 2. Subir o ambiente
docker compose up --build -d

# 3. Acessar a UI e ativar o DAG
# http://localhost:8080  (usuário: airflow / senha: airflow)
# O DAG começa pausado — ative o toggle na UI ou rode:
docker compose exec airflow-scheduler airflow dags unpause openfda_adverse_events

# 4. Disparar manualmente com data histórica (datas em 2025+ não têm dados na API)
docker compose exec airflow-scheduler \
  airflow dags trigger openfda_adverse_events --exec-date 2024-03-15T10:00:00+00:00

# 5. Verificar os dados carregados
docker compose exec app-postgres \
  psql -U appuser -d pharma_db -c "SELECT * FROM adverse_events LIMIT 10;"
```
