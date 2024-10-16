from airflow import DAG
import lifetouch.commons.pipeline_utils as lt
from datetime import datetime, timedelta

# DAGs default args
default_args = {
    'owner': 'lifetouch',
    "start_date": datetime(2024, 2, 13),
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "weight_rule": "upstream",
}


# read DAG configuration
dag_config_file = "64_load_apy_booking_fact_pkg/64_load_apy_booking_fact_pkg_pipeline_config.yaml"
dag_pipeline_config = lt._read_pipeline_config(dag_config_file)
main_dag_id = dag_pipeline_config['name']
schedule = f"{dag_pipeline_config['schedule']}"
pipeline_tags = dag_pipeline_config['tags']
task_groups = dag_pipeline_config['task_groups']
dag_tasks = []

with DAG(
        dag_id = main_dag_id,
        start_date = datetime(2023, 1, 1),
        catchup = False,
        max_active_runs = 1,
        schedule_interval= schedule,
        tags = pipeline_tags,
) as dag:
    
    full_load = lt.check_full_load(dag)
    task_groups[0]['full_load'] = str(full_load)
    #delay to be 60
    shfly_delay_sensor = lt.create_delay_sensor(delay_time=30)
   
    # ingestion_task_group = lt.ingestion_task_group(dag, task_groups[0])

    transformation_task_group = lt.transformation_task_group(dag, task_groups[0]) #This package has no ingestion task group as it has no tables to be ingested

    # shfly_delay_sensor >> ingestion_task_group >> transformation_task_group
    shfly_delay_sensor >> transformation_task_group


