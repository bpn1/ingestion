from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

dag_name = 'ingestion_pipeline'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

wiki_jar = '/home/hadoop/WikiImport/target/scala-2.10/WikiImport-assembly-1.0.jar'
datalake_jar = '/home/hadoop/DataLakeImport/target/scala-2.10/DataLakeImport-assembly-1.0.jar'
programs = [
    ('WikiDataRDD', wiki_jar),
    ('TagEntities', wiki_jar),
    ('ResolveEntities', wiki_jar),
    ('DataLakeImport', wiki_jar),
    ('FindRelations', wiki_jar)]


def make_operator_chain(programList, dag, parent_operator=None):
    last_operator = parent_operator
    for program, jar_path in programList:
        operator = BashOperator(
            task_id=program,
            bash_command='spark.sh yarn ' + program + ' ' + jar_path,
            dag=dag)
        if last_operator is not None:
            operator.set_upstream(last_operator)
        last_operator = operator


dag = DAG(dag_name, default_args=default_args)
make_operator_chain(programs, dag)

print('ingestion_pipeline: End of DAG definition reached.')
