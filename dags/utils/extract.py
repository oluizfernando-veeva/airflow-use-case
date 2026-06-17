from airflow.providers.http.hooks.http import HttpHook


def extract_adverse_events(**context):
    # **context é injetado automaticamente pelo Airflow em toda função de PythonOperator.
    # Contém metadados da execução: datas, task instance, dag, etc.

    # HttpHook é uma abstração do Airflow pra fazer requisições HTTP.
    # Em vez de hardcodar a URL aqui, ele busca host/schema na conexão 'openfda_api'
    # que está configurada via variável de ambiente no compose.yaml.
    hook = HttpHook(http_conn_id='openfda_api', method='GET')

    # data_interval_start é o início do intervalo de tempo que essa execução representa.
    # Num schedule @hourly, cada run representa 1 hora. Se o run é de 14h, data_interval_start = 14h.
    # Isso é o que permite reprocessar datas históricas: cada run tem sua própria janela de tempo.
    date_str = context['data_interval_start'].strftime('%Y%m%d')

    response = hook.run(
        endpoint='/drug/event.json',
        data={'search': f'receivedate:{date_str}', 'limit': 10},
        extra_options={'check_response': False},  # desativa exceção automática pra tratar o 404 manualmente
    )

    if response.status_code == 404:
        # a API retorna 404 quando não tem dados pra aquela data (ex: futuro ou data sem registros)
        return []

    response.raise_for_status()
    # o valor retornado aqui fica disponível via XCom pra próxima tarefa puxar
    return response.json().get('results', [])
