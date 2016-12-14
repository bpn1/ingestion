from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
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

dag = DAG('ingestion_pipeline', default_args=default_args)

t1 = BashOperator(
    task_id='WikiDataRDD',
    bash_command='spark.sh yarn WikiDataRDD /home/hadoop/WikiImport/target/scala-2.10/WikiImport-assembly-1.0.jar',
    dag=dag)

t2 = BashOperator(
    task_id='TagEntities',
    bash_command='spark.sh yarn TagEntities /home/hadoop/WikiImport/target/scala-2.10/WikiImport-assembly-1.0.jar',
    dag=dag)

t3 = BashOperator(
    task_id='ResolveEntities',
    bash_command='spark.sh yarn ResolveEntities /home/hadoop/ApplicationJoin/target/scala-2.10/ApplicationJoin-assembly-1.0.jar',
    dag=dag)

t4 = BashOperator(
    task_id='DataLakeImport',
    bash_command='spark.sh yarn DatalakeImport /home/hadoop/DataLakeImport/target/scala-2.10/DataLakeImport-assembly-1.0.jar',
    dag=dag)

t5 = BashOperator(
    task_id='FindRelations',
    bash_command='spark.sh yarn FindRelations /home/hadoop/DataLakeImport/target/scala-2.10/DataLakeImport-assembly-1.0.jar',
    dag=dag)

t6 = BashOperator(
    task_id='CSVExport',
    bash_command='spark.sh yarn CSVExport /home/hadoop/DataLakeImport/target/scala-2.10/DataLakeImport-assembly-1.0.jar',
    dag=dag)

t7 = BashOperator(
    task_id='cassandra2neo4j',
    bash_command='spark.sh yarn cassandra2neo4j /home/hadoop/Cassandra2Neo4J/target/scala-2.10/cassandra2neo4j-assembly-1.0.jar',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)

print('End of DAG definition reached.')
