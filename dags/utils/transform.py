def transform_data(**context):
    # xcom_pull busca o valor retornado pela tarefa 'extract_data' nessa mesma execução.
    # XCom (cross-communication) é o sistema do Airflow pra tarefas passarem dados entre si.
    # Internamente, os valores ficam salvos no banco de metadados do Airflow (o postgres principal).
    raw_data = context['ti'].xcom_pull(task_ids='extract_data')

    if not raw_data:
        # retornar lista vazia faz o branch escolher 'skip' em vez de 'load'
        return []

    transformed = []
    for record in raw_data:
        # a API da FDA devolve JSON aninhado. Exemplo de estrutura:
        # {
        #   "safetyreportid": "123",
        #   "receivedate": "20240315",
        #   "serious": 1,
        #   "patient": {
        #     "drug": [{"medicinalproduct": "ASPIRINA"}, ...],
        #     "reaction": [{"reactionmeddrapt": "Nausea"}, ...]
        #   }
        # }
        drugs = record.get('patient', {}).get('drug', []) or []
        reactions = record.get('patient', {}).get('reaction', []) or []

        transformed.append({
            'receive_date': record.get('receivedate'),
            'report_id': record.get('safetyreportid'),
            'serious': int(record.get('serious', 0)),
            # achata a lista de medicamentos numa string separada por vírgula
            'drugs': ', '.join(
                d.get('medicinalproduct', '') for d in drugs if d.get('medicinalproduct')
            ),
            # achata a lista de reações numa string separada por vírgula
            'reactions': ', '.join(
                r.get('reactionmeddrapt', '') for r in reactions if r.get('reactionmeddrapt')
            ),
        })

    # o valor retornado fica disponível via XCom pra tarefa 'load_to_postgres' puxar
    return transformed
