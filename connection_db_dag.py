'''
TASK ID: OT303-37

COMO: Analista de datos
QUIERO: Configurar los log
PARA: Mostrarlos en consola

Criterios de aceptación: 

Configurar logs para Universidad De Flores

Configurar logs para Universidad Nacional De Villa María

Utilizar la librería de Loggin de python: https://docs.python.org/3/howto/logging.html

Realizar un log al empezar cada DAG con el nombre del logger

Formato del log: %Y-%m-%d - nombre_logger - mensaje
'''

from airflow.operators.python_operator import PythonOperator
from airflow import DAG

from datetime import timedelta, datetime
from config.cfg import LOG_CFG, LOG_DB
from config.common_args import default_args
from db.db_connection import connection_db
from utils.logger import create_logger_from_file, get_logger


# NOW WE NEED A LOG WITH A ROTATIVE SCHEDULER, SO WE USE (OPT 2)

# Create and configure logger (OPT 1)
#from utils.logger import create_logger
#from config.cfg import LOGS_PATH
#log_name = LOG_DB + '-' + datetime.today().strftime('%Y-%m-%d')
#logger = create_logger(log_name, LOGS_PATH)

# Create and configure logger (OPT 2)
create_logger_from_file(LOG_CFG)
logger = get_logger(LOG_DB)

# Configure DAG parameters.
with DAG(
        'connection_db_dag',
        default_args=default_args,
        description='Retry connection task.',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 9, 16),
        tags=['retry_connection']
) as dag:

    # Make the connection and return Engine.
    connection_task = PythonOperator(
        task_id='connection',
        python_callable=connection_db,
        retries=5,  # Number of times to retry THIS task if fail.
        retry_delay=timedelta(minutes=1),
    )
    connection_task
