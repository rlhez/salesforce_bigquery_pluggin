import json
from tempfile import NamedTemporaryFile


from airflow.models import BaseOperator

from operator import itemgetter


from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.bigquery_hook import BigQueryBaseCursor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


from googleapiclient.errors import HttpError
from airflow import AirflowException




class SalesforceSchemaToBigQueryOperator(BaseOperator):
    """
    Reconcile Schema between salesforce API objects and Redshift

        Leverages the Salesforce API function to describe source objects
        and push the object attributes and datatypes to Redshift tables
        1 table per object

        Ignores Compound Fields as they are already broken out into their
        components else where in the
        :param sf_conn_id:      The conn_id for your Salesforce Instance

        :param gcs_conn_id:      The conn_id for your S3

        :param bq_conn_id:      The conn_id for your Redshift Instance

        :param sf_object:       The Salesforce object you wish you reconcile
                                the schema for.
                                Examples includes Lead, Contacts etc.

        :param bq_dataset:       The schema where you want to put the renconciled
                                schema

        :param bq_table:        The table inside of the schema where you want to
                                put the renconciled schema

        :param gcs_bucket:       The s3 bucket name that will be used to store the
                                JSONPath file to map source schema to redshift columns

        :param gcs_key:          The s3 key that will be given to the JSONPath file

        .. note::
            Be aware that JSONPath files are used for the column mapping of source
            objects to destination tables

            Datatype conversiona happen via the dt_conv dictionary
    """

    dt_conv = {
        'boolean': lambda x: 'BOOL',
        'date': lambda x: 'DATE',
        'dateTime': lambda x: 'TIMESTAMP',
        'double': lambda x: 'NUMERIC',
        'email': lambda x: 'STRING',
        'id': lambda x: 'STRING',
        'ID': lambda x: 'STRING',
        'int': lambda x: 'INT64',
        'picklist': lambda x: 'STRING',
        'phone': lambda x: 'STRING',
        'string': lambda x: 'STRING',
        'textarea': lambda x: 'STRING',
        'url': lambda x: 'STRING'
    }

    template_fields = ('gcs_key',)

    def __init__(self,
                 sf_conn_id, gcs_conn_id, bq_conn_id, # Connection Ids
                 sf_object, # SF Configs
                 bq_project, bq_dataset,bq_table, # RS Configs
                 gcs_bucket, gcs_key, #S3 Configs
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.bq_conn_id = bq_conn_id
        self.sf_object = sf_object
        self.bq_project = bq_project
        self.bq_dataset= bq_dataset
        self.bq_table = bq_table
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key

        super().__init__(*args, **kwargs)

    def fetch_sf_columns(self, sf_conn_id, sf_object):
        """
        Uses Salesforce describe() method to fetch columns from
        Salesforce instance. Compound columns are filtered out.
        """
        sf_conn = SalesforceHook(sf_conn_id).get_conn()

        # Dynamically Fetch the simple_salesforce query method
        # ie. sf_conn.Lead.describe() | sf_conn.Contact.describe()
        sf_fields = sf_conn.__getattr__(sf_object).describe()['fields']

        # Get compound fields
        k1 = 'compoundFieldName'
        compound_fields = [f[k1] for f in sf_fields] # Get all compound fields across all fields
        compound_fields = set(compound_fields)
        compound_fields.remove(None)

        def make_col_ddl(var_type):
            type_transform = self.dt_conv[var_type] # Grab lambda type converter
            rs_type = type_transform(var_type) # Execute type converter
            return rs_type

        def build_dict(x): return {
            'name': x['name'].lower(),
            'sf_name': x['name'],
            'path': [x['name']],
            'sf_type': x['soapType'].split(':')[-1],
            'length': x['length'],
            'precision': x['precision'],
            'type':make_col_ddl(x['soapType'].split(':')[-1])
        }

        sf_cols = [build_dict(field) for field in sf_fields if field['name'] not in compound_fields]

        return sf_cols

    def create_tbl_ddl(self, bq_table, schema):
        """
        Creates the Create Table DDL to be executed on the Redshift
        instance. Only run if table does not exist at time of first run.
        """

        bq_service = BigQueryHook(bigquery_conn_id=self.bq_conn_id).get_service()

        table_resource = {
            'tableReference': {
                'tableId': bq_table
            }
        }

        table_resource['schema'] = {'fields': schema + [{"name":"partition_date","type":"DATE","mode":"NULLABLE"}]}
        table_resource['timePartitioning'] = {"type":"DAY","field":"partition_date"}


        try:
            bq_service.tables().insert(
            projectId=self.bq_project,
            datasetId=self.bq_dataset,
            body=table_resource).execute(num_retries=5)

            self.log.info('Table created successfully: %s:%s.%s',
                          self.bq_project, self.bq_dataset, bq_table)

        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content)
            )


    def patch_bq_cols(self, bq_table,  sf_cols):
        """
        Used to decide whether we need to run an ALTER or CREATE
        table command. Leverages alter_tbl_ddl() and create_tbl_ddl()
        to create the DDL that will be run.
        """
        bq_service = BigQueryHook(bigquery_conn_id=self.bq_conn_id).get_service()
        bq_conn = BigQueryBaseCursor(bq_service,self.bq_project)

        missing_cols = []

        try:
            bq_cols = bq_conn.get_schema(self.bq_dataset,bq_table)
            print(bq_cols)
            bq_cols = [col for col in bq_cols['fields']]
            missing_cols = [x for x in sf_cols if x['name'] not in bq_cols] 

        except:
            bq_cols = []
            for col in sf_cols:
                bq_cols.append({"type":col['type'],"name":col["name"].lower(),"mode":"NULLABLE"})

            self.create_tbl_ddl(bq_table, bq_cols)


        if missing_cols:

            bq_cols = []
            for col in sf_cols:
                bq_cols.append({"type":col['type'],"name":col["name"].lower(),"mode":"NULLABLE"})
            bq_cols.append({"name":"partition_date","type":"DATE","mode":"NULLABLE"})

            print('new schema is ' + str(bq_cols))

            table_resource = {}

            table_resource['schema'] = {'fields': bq_cols}

            try:
                bq_service.tables().patch(
                projectId=self.bq_project,
                datasetId=self.bq_dataset,
                tableId=bq_table,
                body=table_resource).execute()

                self.log.info('Table patched successfully')


            except HttpError as err:
                raise AirflowException('BigQuery job failed. Error was: {}'.format(err.content))

        return bq_cols

    def execute(self, context):
        """
        See class definition.
        """
        # Get Columns From Salesforce
        sf_cols = self.fetch_sf_columns(self.sf_conn_id, self.sf_object)

        print('this is SF data')
        print(sf_cols)

        self.xcom_push(context, key='sf_cols', value=[col['sf_name'] for col in sf_cols])

        # Get Columns From Redshift
        #bq_cols = self.fetch_bq_columns(self.bq_table)
        bq_cols = self.patch_bq_cols(self.bq_table, sf_cols)


        gcs = GoogleCloudStorageHook(self.gcs_conn_id)

        with NamedTemporaryFile("w") as tmp:

            tmp.file.write(str(bq_cols).replace("'",'"'))
            tmp.file.flush()

            gcs.upload(bucket=self.gcs_bucket, object=self.gcs_key, filename=tmp.name)

            tmp.close()        

        self.xcom_push(context,key='bq_cols',value=str(bq_cols))
