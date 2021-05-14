# fn-qry-bgq

import os
from google.cloud import bigquery, logging
from google.cloud import storage
from google.cloud.logging.resource import Resource
from datetime import datetime


def get_folder(value):
    path = '/'.join(value.split('/')[0:-1])
    return path


def bigquery_job(job_name, bucket, file, dataset, table, truncate):
    global logger, grupo_log, hora_ini, res
    try:
        #Inicio carga
        hora_ini = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        #Configuracoes Estrutura Logging
        log_name = 'log_data_ingestion'
        grupo_log = 'DATA_INGESTION'

        #Cria o objeto logger para o log de carga
        res = Resource(type="cloud_function",
                       labels={
                                 "function_name": job_name,
                                 "region": "us-central1"
                        }
        )

        logging_client = logging.Client()
        logger = logging_client.logger(log_name)

        stg_client = storage.Client()
        bkt = stg_client.get_bucket(bucket)
        sql = bkt.get_blob(file).download_as_string().decode('utf-8')

        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.get_dataset(dataset)
        table_ref = dataset_ref.table(table)
        table_id = bigquery_client.get_table(table_ref)

        job_config = bigquery.QueryJobConfig()
        if truncate == 'true':
            sql_truncate = 'truncate table ' + dataset + '.' + table
            query_job = bigquery_client.query(sql_truncate, location='US')
            query_job.result()
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job_config.schema = table_id.schema
        job_config.use_legacy_sql = False
        job_config.destination = table_ref
        job_config.create_disposition = 'CREATE_NEVER'

        query_job = bigquery_client.query(sql, location='US', job_config=job_config)
        query_job.result()
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Sucesso', 'Nome_Job': job_name,
                           'Dataset': dataset, 'Tabela': table, 'Inicio': hora_ini, 'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                            severity='INFO', resource=res)

    except Exception as ex:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': job_name,
                           'Dataset': dataset, 'Tabela': table, 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': str(ex)},
                          severity='ERROR', resource=res)


def query_to_bigquery(request):
    bigquery_job(os.environ['job_name'], os.environ['bucket'], os.environ['file'], os.environ['dataset'], os.environ['table'],
                     os.environ['truncate'])

    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Processado com sucesso!'

# ------------------MAIN-------------------------
#
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\FCA_DATA LAKE\\key\\gcp-data-lake-brain.json"
# os.environ['job_name'] = "fn_stg_carteira_pedido_comercial_vdi"
# #os.environ['folder'] = 'scripts'
# os.environ['bucket'] = 'etl-data-lake-brain-non-prod'
# os.environ['file'] = 'scripts/query/tb_ibm_carteira_pedido_comercial.sql'
# os.environ['dataset'] = 'Staging'
# os.environ['table'] = 'tb_ibm_carteira_pedido_comercial'
# os.environ['truncate'] = 'false'
#
# #event = {"name": "trigger/teste.txt", "bucket": "entrada-data-lake-brain-non-prod"}
# query_to_bigquery("")