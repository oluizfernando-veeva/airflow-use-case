from airflow.providers.http.hooks.http import HttpHook


def extract_adverse_events(**context):
    hook = HttpHook(http_conn_id='openfda_api', method='GET')

    date_str = context['data_interval_start'].strftime('%Y%m%d')

    response = hook.run(
        endpoint='/drug/event.json',
        data={'search': f'receivedate:{date_str}', 'limit': 10},
        extra_options={'check_response': False},
    )

    if response.status_code == 404:
        return []

    response.raise_for_status()
    return response.json().get('results', [])
