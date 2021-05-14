# fn_file_fixed_width_bgq

from google.cloud import bigquery, logging, pubsub_v1, firestore
from google.cloud.logging.resource import Resource
from smart_open import open
import os
import pandas as pd
import io
from datetime import datetime
import json
from google.cloud import storage
import logging as log

PS = pubsub_v1.PublisherClient()
DB = firestore.Client()

def get_folder(value):
    path = '/'.join(value.split('/')[0:-1])
    return path

def get_filename(value):
    return value.split('/')[-1:][0]

def file_to_bigquery(event, context):
    db_ref = DB.document(u'streaming_files/%s' % file_name)
    if get_folder(event['name']) == os.environ['folder']:
        file_name = event['name']
        transfer_file(os.environ['job_name'], file_name, os.environ['bucket'], event['bucket'], os.environ['file_schema'], os.environ['dataset'], os.environ['table'],os.environ['project_id'],os.environ['ignore_header'],os.environ['ignore_footer'])
        _handle_success(db_ref)
    else:
        log.info(
            'ARQUIVO {} NAO CORRESPONDE AO FOLDER {}'.format(get_filename(event['name']), get_folder(event['name'])))

def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def transfer_file(job_name, file, bucket, bucket_file, file_schema, dataset, table, project_id, ignore_header, ignore_footer):
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

        #instancia o client do bigquery
        bigquery_client = bigquery.Client()
        #identifica o dataset e a tabela de destino
        dataset_ref = bigquery_client.get_dataset(dataset)
        table_ref = dataset_ref.table(table)
        table_id = bigquery_client.get_table(table_ref)
        table_name = table_ref.dataset_id + '.' + table_ref.table_id
        #pega o schema da tabela de destino
        table_schema = []
        columns = []
        col_without_dat_ins = []
        for schema in table_id.schema:
            aux = {'name': schema.name, 'type': schema.field_type}
            table_schema.append(aux)
            columns.append(schema.name)

        #recupera o schema do arquivo
        arq_schema = 'gs://' + bucket + '/' + file_schema
        schema_arq = json.load(open(arq_schema))
        colspecs = []

        #Posição dos campos
        for pos_col in schema_arq:
            colspecs.append(pos_col['colspecs'])

        #Cabeçalho
        header = []
        for cab in schema_arq:
            header.append(cab['column'])

        #Leitura do arquivo no storage
        stg_client = storage.Client()
        bkt = stg_client.get_bucket(bucket_file)
        temp_file = bkt.get_blob(file)

        #le o arquivo para memoria
        df = pd.read_fwf(io.StringIO(temp_file.download_as_string().replace(b'\x00', b' ').replace(b'\x01', b' ').decode('utf-8', 'replace')),
                         colspecs=colspecs,
                         names=header,
                         dtype='str',
                         skiprows=int(ignore_header),
                         skipfooter=int(ignore_footer))

        #renomeia as colunas conforme arquivo schema
        col_without_dat_ins = list(columns)
        if 'data_insercao' in columns:
            col_without_dat_ins.remove('data_insercao')
        else:
            col_without_dat_ins.remove('dat_insercao')
        df.columns = col_without_dat_ins

        #Data de inserção
        dt_ins = datetime.now()
        if 'data_insercao' in columns:
            df['data_insercao'] = dt_ins.strftime("%Y-%m-%d %H:%M:%S")
        else:
            df['dat_insercao'] = dt_ins.strftime("%Y-%m-%d %H:%M:%S")

        #reordena as colunas conforme tabela
        df = df[columns]

        #insere os dados no bigquery
        df.to_gbq(destination_table=table_name,
                  chunksize=None,
                  if_exists='append',
                  table_schema=table_schema,
                  project_id=project_id
                  )
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Sucesso', 'Nome_Job': job_name,
                           'Dataset': dataset, 'Tabela': table, 'Inicio': hora_ini, 'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                            severity='INFO', resource=res)

    except Exception as ex:
        logger.log_struct({'Grupo_Log': grupo_log, 'Status_Execucao': 'Erro', 'Nome_Job': job_name,
                           'Dataset': dataset, 'Tabela': table, 'Inicio': hora_ini,
                           'Fim': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                           'Msg_Erro': str(ex)},
                           severity='ERROR', resource=res)


# ------------------MAIN-------------------------
#
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\TESTE\\GCP\\key-fca-shared-digital.json"
#
# os.environ['job_name'] = "fn_file_fixed_width_bgq_ibm_marca_mvs_teste"
# os.environ['bucket'] = "etl-data-lake-brain-non-prod"
# os.environ['file_schema'] = "config/schemas/ibm_marca_mvs_schema.json"
# os.environ['dataset'] = "Staging"
# os.environ['table'] = "ibm_marca_mvs"
# os.environ['folder'] = "ibm_marca_mvs"
# os.environ['project_id'] = "data-lake-brain-non-prod"
# os.environ['ignore_header'] = "0"
# os.environ['ignore_footer'] = "0"
#
# event = {"name": "ibm_marca_mvs/ibm_marca_mvs_20200526000318_J428.D001.LO.FIASA.TA4002.txt", "bucket": "entrada-data-lake-brain-non-prod"}
# file_to_bigquery(event, '')

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\Eder\\FCA_STELLANTIS\\DOCS\\eder-marinho-fca-shared-digital.json"
# os.environ['job_name'] = "fn_file_fixed_width_sap_limite_credito_bgq"
# os.environ['bucket'] = 'etl-data-lake-brain-non-prod'
# #os.environ['file'] = 'scripts/query/fato_sap_limite_credito.sql'
# #os.environ['file'] = 'sap_limite_credito/IF_BR_01_343_S721_20210414-095040-573.txt'
# os.environ['file_schema'] = "config/schemas/sap_limite_credito_schema.json"
# os.environ['dataset'] = 'Staging'
# os.environ['table'] = 'tb_sap_limite_credito'
# os.environ['folder'] = 'sap_limite_credito'
# #os.environ['truncate'] = 'false'
# os.environ['project_id'] = "data-lake-brain-non-prod"
# os.environ['ignore_header'] = "1"
# os.environ['ignore_footer'] = "1"
# #
# event = {"name": "sap_limite_credito/IF_BR_01_343_S721_20210414-095040-573.txt", "bucket": "entrada-data-lake-brain-non-prod"}
# #query_to_bigquery("")
# file_to_bigquery(event, '')