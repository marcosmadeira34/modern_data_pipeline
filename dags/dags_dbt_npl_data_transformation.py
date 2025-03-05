# dbt NPL Data using Astronomer Cosmos 

import os
import logging
from datetime import datetime 
from pathlib import Path

from airflow.decorators import dag 
from cosmos.profiles import DatabricksTokenProfileMapping 
from cosmos import (DbtTaskGroup, ProfileConfig, ProjectConfig)


logger = logging.getLogger(__name__)
doc_md = """
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

default_dbt_root_path = Path(__file__).parent / 'dbt'
dbt_root_path = Path(os.getenv('DBT_ROOT_PATH', default_dbt_root_path))

@dag(
    default_args=default_args,
)


def my_dag():
    pass