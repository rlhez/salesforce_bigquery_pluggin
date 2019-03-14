from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from plugins.salesforce_plugin.operators.salesforce_to_gcs_operator import SalesforceToGCSOperator
from plugins.salesforce_plugin.operators.salesforce_schema_to_bigquery_operator import SalesforceSchemaToBigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.models import Variable

SF_CONN_ID = 'salesforce_conn'
GCS_CONN_ID = 'google_cloud_default'
BIGQUERY_CONN_ID = 'bigquery_default'
BIGQUERY_PROJECT_NAME = 'my-project'

SCHEMA_LOCATION = 'gcs'
MAX_ACTIVE_RUNS = 1

biquery_dataset = Variable.get("BIGQUERY_DATASET")
gcs_bucket_name = Variable.get("GCS_BUCKET_NAME")


default_args = {
    'owner': 'rlhez',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 12),
    'email': ['romain.lhez@360learning.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('salesforce_to_bigquery',
          default_args=default_args,
          catchup=False,
          schedule_interval='@daily',
          max_active_runs=MAX_ACTIVE_RUNS)

tables = [{'name': 'Contact',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {'name': 'Account',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},{
           'name': 'Contact',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'
           },{
           'name': 'Opportunity',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'OpportunityLineItem',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'Product2',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'OpportunityContactRole',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'Contract',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'Contract',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'Profile',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},
           {
           'name': 'User',
           'load_type': 'upsert',
           'primary_key': 'id',
           'incremental_key': 'createddate'},



           ]

kick_off_dag = DummyOperator(task_id='kick_off_dag',
                             dag=dag)


for table in tables:


    OBJECT = table['name']

    GCS_KEY_SCHEMA = 'salesforce/{0}/schemas/{1}.json'.format('{{ ts_nodash }}',OBJECT.lower()+'_schema')

    sfdc_schema_task_id = '{}_sync_schema'.format(OBJECT)

    sync_schema = SalesforceSchemaToBigQueryOperator(
        task_id=sfdc_schema_task_id,
        sf_conn_id=SF_CONN_ID,
        gcs_conn_id=GCS_CONN_ID,
        bq_conn_id=BIGQUERY_CONN_ID,
        sf_object=OBJECT,
        bq_project=BIGQUERY_PROJECT_NAME,
        bq_dataset= biquery_dataset,
        bq_table='sf_'+OBJECT.lower(),
        gcs_key=GCS_KEY_SCHEMA,
        gcs_bucket=gcs_bucket_name,
        dag=dag
        )

    SF_COLS = "{{ ','.join(task_instance.xcom_pull(task_ids='" + sfdc_schema_task_id + "', key='sf_cols')) }}"
    #ORIGIN_SCHEMA = "{{ task_instance.xcom_pull(task_ids='" + sfdc_schema_task_id + "', key='output_schema') }}"
    BQ_SCHEMA = "{{ task_instance.xcom_pull(task_ids='" + sfdc_schema_task_id + "', key='bq_cols') }}"

    

    """
    Set the lower and upper bounds for the Salesforce queries as the previous
    and current execution dates. If there is not previous execution date, the
    lower bound will be set to the current execution timestamp. These are then
    passed to the Date Filter. If the object has a specified incremental key,
    that will be used. Otherwise, it will default ot SystemModstamp if
    incremental_key cannot be found.
    """

    UPPER_BOUND = "{{ ts + 'Z' }}"
    DATE_FILTER = "WHERE {incr_key} < {uppr_lmt}".format(
        incr_key=table.get('incremental_key', 'SystemModstamp'),
        uppr_lmt=UPPER_BOUND
    )

    INCR_SOQL = "SELECT {sf_columns} FROM {sf_object} {date_filter}".format(sf_columns=SF_COLS,
                                                                            sf_object=OBJECT,
                                                                            date_filter=DATE_FILTER)




    query_id = '{0}_to_gcs'.format(OBJECT)

    GCS_KEY = 'salesforce/{0}/{1}.json'.format('{{ ts_nodash }}', OBJECT.lower()+'_records')

    full_query = SalesforceToGCSOperator(
        task_id=query_id,
        sf_conn_id=SF_CONN_ID,
        object_type=OBJECT,
        soql=INCR_SOQL,
        gcs_conn_id=GCS_CONN_ID,
        gcs_bucket=gcs_bucket_name,
        gcs_object=GCS_KEY,
        bq_shema = BQ_SCHEMA,
        dag=dag
    )

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(task_id='{0}_to_BigQuery'.format(table['name']),
    												      bucket=gcs_bucket_name,
									                source_objects=[GCS_KEY],
									                destination_project_dataset_table=BIGQUERY_PROJECT_NAME+'.'+biquery_dataset+'.sf_'+OBJECT.lower(),
									                schema_object=GCS_KEY_SCHEMA,
									                source_format='NEWLINE_DELIMITED_JSON',
									                compression='NONE',
									                create_disposition='CREATE_IF_NEEDED',
									                skip_leading_rows=0,
									                write_disposition='WRITE_APPEND',
									                field_delimiter=',',
									                bigquery_conn_id=BIGQUERY_CONN_ID,
									                google_cloud_storage_conn_id=GCS_CONN_ID,
									                delegate_to=None,
									                schema_update_options=(),
									                src_fmt_configs={'skipLeadingRows':0},
									                external_table=False,
									                time_partitioning={"type":"DAY","field":"partition_date"},
									                cluster_fields=None,
									                autodetect=False,
									                dag=dag)


    kick_off_dag >> sync_schema >> full_query >> gcs_to_bq

