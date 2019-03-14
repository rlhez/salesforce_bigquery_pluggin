from tempfile import NamedTemporaryFile
import logging
import json
import pandas as pd
import re

from datetime import datetime,date

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from airflow.contrib.hooks.salesforce_hook import SalesforceHook

from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError



class SalesforceToGCSOperator(BaseOperator):
    """
        Queries the Salesforce Bulk API using a SOQL stirng. Results are then
        put into an S3 Bucket.

    :param sf_conn_id:      Salesforce Connection Id
    :param soql:            Salesforce SOQL Query String used to query Bulk API
    :param: object_type:    Salesforce Object Type (lead, contact, etc)
    :param s3_conn_id:      S3 Connection Id
    :param s3_bucket:       S3 Bucket where query results will be put
    :param s3_key:          S3 Key that will be assigned to uploaded Salesforce
                            query results
    """
    template_fields = ('soql','gcs_object')

    def __init__(self,
                 sf_conn_id,
                 soql,
                 object_type,
                 gcs_conn_id,
                 gcs_bucket,
                 gcs_object,
                 bq_shema,
                 fmt="json",
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.soql = soql
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_object = gcs_object
        self.bq_shema = bq_shema
        self.fmt = fmt.lower()
        self.object = object_type[0].upper() + object_type[1:].lower()

    def upload(self, service ,bucket, object, filename,
               mime_type='application/octet-stream', gzip=False,
               multipart=False, num_retries=0):
        """
        Uploads a local file to Google Cloud Storage.
        :param bucket: The bucket to upload to.
        :type bucket: str
        :param object: The object name to set when uploading the local file.
        :type object: str
        :param filename: The local file path to the file to be uploaded.
        :type filename: str
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: str
        :param gzip: Option to compress file for upload
        :type gzip: bool
        :param multipart: If True, the upload will be split into multiple HTTP requests. The
                          default size is 256MiB per request. Pass a number instead of True to
                          specify the request size, which must be a multiple of 262144 (256KiB).
        :type multipart: bool or int
        :param num_retries: The number of times to attempt to re-upload the file (or individual
                            chunks, in the case of multipart uploads). Retries are attempted
                            with exponential backoff.
        :type num_retries: int
        """

        if gzip:
            filename_gz = filename + '.gz'

            with open(filename, 'rb') as f_in:
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz

        try:
            if multipart:
                if multipart is True:
                    chunksize = 256 * 1024 * 1024
                else:
                    chunksize = multipart

                if chunksize % (256 * 1024) > 0 or chunksize < 0:
                    raise ValueError("Multipart size is not a multiple of 262144 (256KiB)")

                media = MediaFileUpload(filename, mimetype=mime_type,
                                        chunksize=chunksize, resumable=True)

                request = service.objects().insert(bucket=bucket, name=object, media_body=media)
                response = None
                while response is None:
                    status, response = request.next_chunk(num_retries=num_retries)
                    if status:
                        self.log.info("Upload progress %.1f%%", status.progress() * 100)

            else:
                media = MediaFileUpload(filename, mime_type)

                service \
                    .objects() \
                    .insert(bucket=bucket, name=object, media_body=media) \
                    .execute(num_retries=num_retries)

        except HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

        finally:
            if gzip:
                os.remove(filename)

        return True

    def execute(self, context):

        with NamedTemporaryFile("w") as tmp:

            # Load the SalesforceHook
            hook = SalesforceHook(conn_id=self.sf_conn_id, output=tmp.name)

            # Attempt to login to Salesforce
            # If this process fails, it will raise an error and die.
            try:
                sf_conn = hook.sign_in()
            except:
                logging.debug('Unable to login.')

            logging.info(self.soql)
            logging.info(self.object)


            logging.debug('Connecting to Salesforce...')
            query_results = sf_conn.bulk.__getattr__(self.object).query(self.soql)
            logging.info('Retrieved results...')

            logging.info(type(query_results))
            logging.info('First line is:')
            logging.info(query_results[0])

            gcs = GoogleCloudStorageHook(self.gcs_conn_id)
            service = gcs.get_conn()

            logging.info('Preparing File...')

            
            intermediate_arr = []

            for i,q in enumerate(query_results):

                del q['attributes']
                q["partition_date"] = date.today().strftime('%Y-%m-%d')

                for k,v in q.items():

                    if (type(v) == float):
                        q[k] = round(v,2)
                    if (type(v) == int) and (len(str(v)) == 13):
                        q[k] = datetime.fromtimestamp(v/1000).strftime('%Y-%m-%d %H:%M:%S')
                    if (type(v) == str) and (re.search(r"^(\d+\.\d+)$",v) != None):
                        q[k] = round(float(v),2)


                        
                for key in q.keys():

                    q[key.lower()] = q.pop(key)

                query = json.dumps(q, ensure_ascii=False)
                intermediate_arr.append(query+'\n')
                del query

                if i % 100 == 0:
                    tmp.file.writelines(intermediate_arr)
                    intermediate_arr = []

                    #tmp.file.write(str(query+'\n'))
            tmp.file.writelines(intermediate_arr)

#            tmp.file.flush()

            logging.info('Loading results to GCS...')
            
            self.upload(
                service=service,
                bucket=self.gcs_bucket,
                filename=tmp.name,
                object=self.gcs_object,
                multipart=True,
                num_retries=2
            )

            tmp.close()

        logging.info("Query finished!")