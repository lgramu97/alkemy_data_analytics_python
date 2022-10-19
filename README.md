# Data Analytics + Python - [Alkemy-Labs](https://www.alkemy.org/)

# Resume
This project have 3 steps:
* **Entry challenge**: [repository](https://github.com/lgramu97/data_challenge_alkemy)  

* **Execution Flows (ETL)**: in this project, as a part of a development and data analytics team, we must analyze and prepare execution flows of the dataset received to obtain the comparisons and measurements required by the CNCE. Technologies: Airflow, Pandas, PostgreSQL, SQLAlchemy, Loggers, S3, Numpy. **Sprint I - Sprint II - Sprint III**.
    * Create SQL queries using a postgresql database.  

    * The data must be processed in such a way that queries can be made to two universities of the total available to carry out partial analyses. For this it will be necessary to make DAGs with Airflow that allow data to be processed with Python and SQL queries.  

    * Calculate, evaluate and normalize data such as dates, names, postal codes according to standardized requirements that are specified for each group of universities, using Pandas.
      
 * **Big Data**: As part of a development and data analytics team, we need to analyze, report, and document the processing of the received dataset to obtain the comparisons and measurements required by the StackOverflow administrator group. Technologies: MapReduce technique, Hadoop, Pytest. **Sprint IV**.
 
#
# SPRINT I
##  TASK ID: OT303-13 (Scripts SQL)
* Folder: /sql. 
* Two .sql files, each one is a script for the corresponding university.

## TASK ID: OT303-21 (ETL DAGs)
* File university_etl_dag.py: main DAG, workflow.
* File university_etl_functions.py: contains the task DAG functons.
* Extract: when the DAG runs, it creates a folder with the csv files as result.
* For this task, create settings.ini file with the necessary information for the postgresql connection. Example:
  * DB_USER=user
  * DB_PASS=pass
  * DB_HOST=some_host
  * DB_PORT=port_number
  * DB_NAME=db_name
  * #S3 settings
  * CONNECTION=s3_conn
  * BUCKET_NAME=bucket_name
* Files used in this task: university_etl_dag.py, university_etl_functions.py, utils.py, db_connection.py, cfg.py, common_args.py and /sql.

## TASK ID: OT303-29 (Retry connection)
* File connection_db_dag.py: retry connection DAG.
* File db_connection.py: contains the task DAG functons.
* Files used in this task: connection_db_dag.py, db_connection.py, cfg.py.

#
# SPRINT II

## TASK ID: OT303-37 (Logs)
* Folder /logs have the logs after run the DAGs.
* File /utils/logger.py: logs implementation.
* Dags updated with logs.

## TASK ID: OT303-45 (Extract)
* Folder /csv have the information extracted in .csv format from the database for University Flores and University Villa Maria.
* university_etl_dag.py (task extract), university_etl_functions.py (funcion extract implementada)

## TASK ID: OT303-61 (Transform Function)
* Folder /extras have a jupyter notebook created to transform the data (spy on data).
* Folder /clear_data have two files txt with the data transformed.
* university_etl_functions.py: implements the transformation function in python.
* transform.py: transform data using pandas.

## TASK ID: OT303-53 (Transform task)
* university_etl_dag.py implements the DAG task for transform operation.

#
# SPRINT III
## TASK ID: OT303-69 y OT303-70 (Load task)
* university_etl_dag.py implements the task upload to S3.
* university_etl_functions.py implements the function upload to S3.

## TASK ID: OT303-93 (Logs from file)
* logger.py creates a logger from a configuration file.
* logging.conf: log, handlers and formatters.
* myHandler.py: custom FileHandler for a rotative log.

## TASK ID: OT303-85 (Dynamic Dag)
* Dynamic dags implementation using dag-factory: pip install dag-factory
* Same business logic. 
* Here another way to implement it: https://towardsdatascience.com/how-to-build-a-dag-factory-on-airflow-9a19ab84084c.
* File config/config_dynamic_dag.yaml defines the Dag structure.
* dynamic_etl_dag.py creates the dag.

## TASK ID: OT303-101 (MapReduce Group A)
* Folder /big_data/map_reduce contains the files of this task.
* mapper_stdin.py and reducer_stdin.py are test examples using standar input/output.
* parsing.py another example file to test parsing xml data.
* top_10_positive_tags.py: Top 10 tags with higher answer accepted.
* avg_wrods_score.py: Relation between number of words and score in a post.
* avg_answer_post.py: Average time answer in post.

#
# SPRINT IV
## TASK ID: OT303-109 y OT303-117 (MapReduce Group A optimization/hadoop)
* Implements mapReduce jobs using mrjob library.
* Run in hadoop: run docker container, install python and the dependencies. Copy the scripts and the input data.
* Local: python3 name_script.py input > output
* Hadoop: pyton3 name_script.py input -r hadoop > output

## TASK ID: OT303:125 (Unit Test)
* PyTest.
* Each function from mrjobs_.py have tests.
* Folder big_data/tests.

## TASK ID: OT303:133 (Documentation)
* All features in this repository were documented using docstrings.
* [Visual Studio Plugin](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
