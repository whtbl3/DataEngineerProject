from datetime import datetime, timedelta
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries, ConfigureDataAccess
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

start_date = datetime.utcnow()
default_args = {
    'owner': 'udacity_learner_phulh27',
    'start_date': start_date,
    'depend_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG('sparkify_dag',
         schedule_interval='0 * * * *',
         description='Load and transform data in Redshift with Airflow',
         default_args=default_args) as dag:

    start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

    with TaskGroup(group_id='load_stage_table') as load_stage_table:
        
        stage_immigration_to_redshift = StageToRedshiftOperator(
            task_id='Stage_immigration',
            dag=dag,
            redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
            table='staging_immigration',
            s3_bucket=ConfigureDataAccess.S3_BUCKET,
            s3_key=ConfigureDataAccess.S3_I94IMMIGRATION_KEY,
            region=ConfigureDataAccess.REGION,
            data_format=ConfigureDataAccess.DATA_FORMAT
        )
        
        stage_demographic_to_redshift = StageToRedshiftOperator(
            task_id='Stage_demographic',
            dag=dag,
            redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            aws_credentials_id=ConfigureDataAccess.AWS_CREDENTIALS_ID,
            table='staging_demographics',
            s3_bucket=ConfigureDataAccess.S3_BUCKET,
            s3_key=ConfigureDataAccess.S3_DEMOGRAPHIC_KEY,
            region=ConfigureDataAccess.REGION,
            data_format=ConfigureDataAccess.DATA_FORMAT
        )
        
    with TaskGroup(group_id='load_dimension_table') as load_dimension_table:
        
        load_cities_dim_table = LoadDimensionOperator(
            task_id="load_cities_dim_table",
            dag=dag,
            table_name='cities',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.cities_table_insert,
            truncate=True
        )
        
        load_states_dim_table = LoadDimensionOperator(
            task_id="load_states_dim_table",
            dag=dag,
            table_name='states',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.states_table_insert,
            truncate=True
        )
        
        load_countries_dim_table = LoadDimensionOperator(
            task_id="load_countries_dim_table",
            dag=dag,
            table_name='countries',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.countries_table_insert,
            truncate=True
        )
        
        load_visa_dim_table = LoadDimensionOperator(
            task_id="load_visa_dim_table",
            dag=dag,
            table_name='visa',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.visa_table_insert,
            truncate=True
        )
        
        load_migrants_dim_table = LoadDimensionOperator(
            task_id="load_migrants_dim_table",
            dag=dag,
            table_name='migrants',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.migrants_table_insert,
            truncate=True
        )
        
        load_demography_dim_table = LoadDimensionOperator(
            task_id="load_demography_dim_table",
            dag=dag,
            table_name='demography',
            postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
            insert_sql_stmt=SqlQueries.demography_table_insert,
            truncate=True
        )
        
        
    load_human_migration_table = LoadFactOperator(
        task_id='Load_human_migration_fact_table',
        dag=dag,
        table_name='human_migration',
        postgres_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        sql_insert_stmt=SqlQueries.human_migration_table_insert
    )
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id=ConfigureDataAccess.REDSHIFT_CONN_ID,
        dq_checks_list=[
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.cities;', 'expected_result': 299 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.states;', 'expected_result': 457 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.countries;', 'expected_result': 243 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.visa;', 'expected_result': 17 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.migrants;', 'expected_result': 3096313 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.demography;', 'expected_result': 596 },
            { 'sql_testcase': 'SELECT COUNT(*) FROM public.human_migration;', 'expected_result': 7255784 },
        ]
    )
    
    end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)
    

start_operator >> load_stage_table  
load_stage_table >> load_human_migration_table >> load_dimension_table
load_dimension_table >> run_quality_checks >> end_operator
    
    