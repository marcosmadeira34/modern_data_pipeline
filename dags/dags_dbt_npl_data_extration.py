import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

default_dbt_root_path = Path(__file__).parent / 'dbt'
dbt_root_path = Path(os.getenv('DBT_ROOT_PATH', default_dbt_root_path))

with DAG(
    'bigquery_loading',
    default_args=default_args,
    description='An example DAG to run a BigQuery query',
    schedule_interval=timedelta(hours=1),  # Execute manual ou defina um cronograma
    start_date=days_ago(1),
    catchup=False, # Não execute tarefas perdidas de execuções anteriores e evita gastar recursos desnecessários
) as dag:

    # Definir consulta SQL
    query = """
        SELECT * FROM `dbt-bigquery-452812-q7.dbtconjuntodedados.olist_products_dataset`
        
    """

    # Job BigQuery para extração dos dados
    bigquery_job = BigQueryInsertJobOperator(
        task_id='extrair_dados',
        configuration={
            'query': {
                'query': query,
                'useLegacySql': False,
                'destinationTable': {
                'projectId': 'dbt-bigquery-452812-q7',  # Seu projeto no BigQuery
                'datasetId': 'dbtconjuntodedados',  # O dataset onde será salva a tabela
                'tableId': 'olist_products_result',  # Nome da tabela de destino
            },
            'writeDisposition': 'WRITE_APPEND',  # Adiciona apenas novos dados, reduzindo custos antes sobrescrevendo com Truncate
        }
    },
    location='US',  # Região onde o BigQuery executa a query
)


    # Operadores Empty (início e fim)
    init_data_load = EmptyOperator(task_id='init')
    finish_data_load = EmptyOperator(task_id='finish')

    # TaskGroup para carregar dados locais
    # with TaskGroup(group_id="Users") as users_task_group:
    #     load_local_files_from_bigquery = aql.load_file(
    #         task_id='load_local_files_from_bigquery',
    #         input_file=File('olist_products_dataset', 'dbtconjuntodedados'),
    #         output_table=Table('olist_products_dataset', 'dbtconjuntodedados'),
    #     )

    # Definir dependências
    init_data_load >> bigquery_job >> finish_data_load
