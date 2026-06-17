def transform_data(**context):
    raw_data = context['ti'].xcom_pull(task_ids='extract_data')

    if not raw_data:
        return []

    transformed = []
    for record in raw_data:
        drugs = record.get('patient', {}).get('drug', []) or []
        reactions = record.get('patient', {}).get('reaction', []) or []

        transformed.append({
            'receive_date': record.get('receivedate'),
            'report_id': record.get('safetyreportid'),
            'serious': int(record.get('serious', 0)),
            'drugs': ', '.join(
                d.get('medicinalproduct', '') for d in drugs if d.get('medicinalproduct')
            ),
            'reactions': ', '.join(
                r.get('reactionmeddrapt', '') for r in reactions if r.get('reactionmeddrapt')
            ),
        })

    return transformed
