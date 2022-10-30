import re
import json
import subprocess
from time import sleep
import apache_beam as beam
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions


class GBQ:

    def __init__(self):
        self.client = bigquery.Client()


    def create(self, table_id, table_schema):

        table = bigquery.Table(table_id, table_schema)

        table = self.client.create_table(table)

        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


    def delete(self, table_id):

        print(f"deleting table {table_id}")
        
        self.client.delete_table(table_id, not_found_ok=True)


    def insert(self, table_id, rows):

        errors = self.client.insert_rows_json(table_id, rows)
        
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))


class GCS:

    def __init__(self):
        self.storage_client = storage.Client()


    def bucket_present(self, bucket_name):
        """
        Is the Bucket present?
        """

        bucket = self.storage_client.bucket(bucket_name)

        return bucket.exists()


    def list_buckets(self):
        """
        List Buckets
        """

        buckets = self.storage_client.list_buckets()
        print("Buckets:")
        for blob in buckets:
            print(blob.name)
        print("Listed all storage buckets.")


    def run(self, cmd):
        print('>> {}'.format(cmd))

        res = subprocess.call(cmd, shell = True)
        print(f'completed create buckert call code = {res}.\n')


    def create_bucket(self, bucket_name, location):
        """
        Create a new bucket
        """
        # --project=PROJECT_ID --default-storage-class=STORAGE_CLASS 

        cmd = f"gcloud storage buckets create gs://{bucket_name} --location={location} --uniform-bucket-level-access"

        self.run(cmd);

        # bucket = self.storage_client.bucket(bucket_name)
        # bucket.storage_class = "STANDARD"
        # bucket.location = location
        # new_bucket = self.storage_client.create_bucket(bucket, location="us")

        # print(
        #     "Created bucket {} in {} with storage class {}".format(
        #         new_bucket.name, new_bucket.location, new_bucket.storage_class
        #     )
        # )
        #return new_bucket


    def delete_bucket(self, bucket_name):
        """Deletes a bucket"""

        if (self.bucket_present(bucket_name)):

            print(f"Bucket {bucket_name} exists")

            bucket = self.storage_client.bucket(bucket_name)

            blobs = self.storage_client.list_blobs(bucket_name)

            for blob in blobs:

                blob_object = bucket.blob(blob.name)
                print(f"Deleting object blob {blob.name}")
                blob_object.delete()
                
            bucket.delete()

            print(f"Bucket {bucket_name} deleted")
        else:
            print(f"Bucket {bucket_name} does not exist")


    def list_blobs(self, bucket_name):
        """
        List Blobs
        """

        blobs = self.storage_client.list_blobs(bucket_name)

        for blob in blobs:
            print(blob.name)


    def upload_file(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )


class Etl:

    def __init__(self, table_id, input_gs, temp_gs, pipeline_options):

        self.table_id           = table_id
        self.input_gs           = input_gs
        self.temp_gs            = temp_gs
        self.pipeline_options   = pipeline_options


    def strip_non_ascii(self, string):

        stripped = (c for c in string if 0 < ord(c) < 127)
        return ''.join(stripped)


    def original_sring_item_type_check(self, x):

        from datetime import datetime

        def isfloat(x):
            try:
                result = float(x)
                return True
            except ValueError:
                return False

        def isEmptyOrFloat(x):
            if (x is None or x.strip() == ""):
                return True
            else:
                return isfloat(x)

        def isEmptyOrDate(x):
            if (x is None or x.strip() == ""):
                return True
            else:
                try:
                    result = datetime.strptime(x, "%Y-%m-%d")
                    return True
                except ValueError:
                    return False

        return  x[0].isnumeric() and x[2].isnumeric() and isfloat(x[6]) and isfloat(x[7]) and  x[9].isnumeric() and \
                x[10].isnumeric() and x[11].isnumeric() and  isEmptyOrDate(x[12]) and  isEmptyOrFloat(x[13]) and   \
                x[14].isnumeric() and x[15].isnumeric()


    def original_string_to_typed_item_conversion(self, x):

        def toEmptyOrFloat(x):

            if (x is None or x.strip() == ""):
                return 0.0
            else:
                return float(x)

        def toEmptyOrDate(x):

            if (x is None or x.strip() == ""):
                return '1900-01-01'
            else:
                return x

        return  [
                    int(x[0]),                  # id" 
                    x[1],                       # "name": ,
                    int(x[2]),                  # "host_id": ,
                    x[3],                       # "host_name":,
                    x[4],                       # "neighbourhood_group": ,
                    x[5],                       # "neighbourhood": ,
                    float(x[6]),                # "latitude":,
                    float(x[7]),                # "longitude": ,
                    x[8],                       # "room_type": ,
                    int(x[9]),                  # "price":,
                    int(x[10]),                 # "minimum_nights": ,
                    int(x[11]),                 # "number_of_reviews": ,
                    toEmptyOrDate(x[12]),       # "last_review":, 
                    toEmptyOrFloat(x[13]),      # "reviews_per_month":,
                    int(x[14]),                 # "calculated_host_listings_count": ,
                    int(x[15])                  # "availability_365": 
                ]

    def map_original_records(self, x):
        
        y = {
            "id": x[0], 
            "name": x[1],
            "host_id": x[2],
            "host_name": x[3],
            "neighbourhood_group": x[4],
            "neighbourhood": x[5],
            "latitude": x[6],
            "longitude": x[7],
            "room_type": x[8],
            "price": x[9],
            "minimum_nights": x[10],
            "number_of_reviews": x[11],
            "last_review": x[12], 
            "reviews_per_month": x[13],
            "calculated_host_listings_count": x[14],
            "availability_365": x[15]
        }

        # print(y)
        # print("=== \n" + json.dumps(y) +  "\n")

        return y


    def run(self):

        import logging

        # logging.getLogger().setLevel(logging.DEBUG)

        queries_schema = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'host_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'host_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'neighbourhood_group', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'neighbourhood', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                {'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                {'name': 'room_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'minimum_nights', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'number_of_reviews', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'last_review', 'type': 'DATE', 'mode': 'NULLABLE'},
                {'name': 'reviews_per_month', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                {'name': 'calculated_host_listings_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'availability_365', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            ]
        }

        with beam.Pipeline(options=self.pipeline_options) as pipeine:
            (
                pipeine
                    # read all lines
                |   'ReadData'              >> beam.io.ReadFromText(self.input_gs, skip_header_lines =1)
                    # take out non ASCII
                |   'Strip non ASCII'       >> beam.Map(self.strip_non_ascii)
                    # process only lines with 16 elements that can be separated by a comma
                |   'Valid Lines'           >> beam.Filter(lambda line : len(line.split(',')) == 16)
                    # split on commas
                |   'Split on comma'        >> beam.Map(lambda x: x.split(','))
                    # check correct type for each column
                |   'Type Check'            >> beam.Filter(self.original_sring_item_type_check)
                |   'Map to Type'           >> beam.Map(self.original_string_to_typed_item_conversion)
                |   'Map to Dictionary'     >> beam.Map(self.map_original_records)
                #|   'Print'                 >> beam.Map(print)

                |   'WriteData' >> beam.io.gcp.bigquery.WriteToBigQuery(
                                                                        table=self.table_id,
                                                                        schema=queries_schema, 
                                                                        write_disposition='WRITE_APPEND',
                                                                        #method='STREAMING_INSERTS',
                                                                        insert_retry_strategy='RETRY_NEVER',
                                                                        custom_gcs_temp_location=self.temp_gs
                                                                    )
            )


class Grouped_Etl:

    def __init__(self, table_id, input_gs, temp_gs, pipeline_options):

        self.table_id           = table_id
        self.input_gs           = input_gs
        self.temp_gs            = temp_gs
        self.pipeline_options   = pipeline_options


    def strip_non_ascii(self, string):

        stripped = (c for c in string if 0 < ord(c) < 127)
        return ''.join(stripped)


    def map_grouped_records(self, x):

        def nested_list_to_dict(z):
            lst = []
            for x in z:    
                lst.append ({

                "id": x[0], 
                "name": x[1],
                "host_id": x[2],
                "host_name": x[3],
                "neighbourhood_group": x[4],
                "latitude": x[6],
                "longitude": x[7],
                "room_type": x[8],
                "price": x[9],
                "minimum_nights": x[10],
                "number_of_reviews": x[11],
                "last_review": x[12], 
                "reviews_per_month": x[13],
                "calculated_host_listings_count": x[14],
                "availability_365": x[15]
                })

            return lst

        key = x[0]
        records = x[1]

        y = {

            "neighbourhood": x[0],
            "grouped_records" : nested_list_to_dict(records)
        }

        # print("=== \n" + json.dumps(y) +  "\n")

        return y


    def grouped_sring_item_type_check(self, x):

        from datetime import datetime

        def isfloat(x):
            try:
                result = float(x)
                return True
            except ValueError:
                return False

        def isEmptyOrFloat(x):
            if (x is None or x.strip() == ""):
                return True
            else:
                return isfloat(x)

        def isEmptyOrDate(x):
            if (x is None or x.strip() == ""):
                return True
            else:
                try:
                    result = datetime.strptime(x, "%Y-%m-%d")
                    return True
                except ValueError:
                    return False

        return x[0].isnumeric() and x[2].isnumeric() and isfloat(x[6]) and isfloat(x[7]) and x[9].isnumeric() and x[10].isnumeric() and \
                x[11].isnumeric() and isEmptyOrDate(x[12]) and isEmptyOrFloat(x[13]) and x[14].isnumeric() and x[15].isnumeric()


    def grouped_string_to_typed_item_conversion(self, x):
        return  [
                int(x[0]),                                                                  # id" 
                x[1],                                                                       # "name": ,
                int(x[2]),                                                                  # "host_id": ,
                x[3],                                                                       # "host_name":,
                x[4],                                                                       # "neighbourhood_group": ,
                x[5],                                                                       # "neighbourhood": ,
                float(x[6]),                                                                # "latitude":,
                float(x[7]),                                                                # "longitude": ,
                x[8],                                                                       # "room_type": ,
                int(x[9]),                                                                  # "price":,
                int(x[10]),                                                                 # "minimum_nights": ,
                int(x[11]),                                                                 # "number_of_reviews": ,
                '1900-01-01' if (x[12] is None or x[12].strip() == "") else x[12],          # "last_review":, 
                0.0 if (x[13] is None or x[13].strip() == "") else float(x[13]),            # "reviews_per_month":,
                int(x[14]),                                                                 # "calculated_host_listings_count": ,
                int(x[15])                                                                  # "availability_365": 
            ]


    def run(self):

        queries_schema = {
            'fields': [
                {'name': 'neighbourhood', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'grouped_records', 'type': 'RECORD', 'mode': 'REPEATED',
                    'fields': [
                        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'host_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'host_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'neighbourhood_group', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                        {'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                        {'name': 'room_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'minimum_nights', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'number_of_reviews', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'last_review', 'type': 'DATE', 'mode': 'NULLABLE'},
                        {'name': 'reviews_per_month', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                        {'name': 'calculated_host_listings_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'availability_365', 'type': 'INTEGER', 'mode': 'NULLABLE'}
                    ]            
                }
            ]
        }

        with beam.Pipeline(options=self.pipeline_options) as pipeine:

            result =  (
                
                pipeine
                    # read all lines
                |   'ReadData'              >> beam.io.ReadFromText(self.input_gs, skip_header_lines=1)
                    # take out non ASCII
                |   'Strip non ASCII'       >> beam.Map(self.strip_non_ascii)
                # process only lines with 16 elements that can be separated by a comma
                |   'Valid Lines'           >> beam.Filter(lambda line : len(line.split(',')) == 16)
                    # split on commas
                |   'Split on comma'        >> beam.Map(lambda x: x.split(','))
                    # check correct type for each column
                |   'Type Check'            >> beam.Filter(self.grouped_sring_item_type_check)
                    # convert string to correct type           
                |   'Map to Type'           >> beam.Map(self.grouped_string_to_typed_item_conversion)
                    # group by Neghborhood field
                |   'Group By'              >> beam.GroupBy(lambda x: x[5])  # "neighbourhood"
                    # change into dictionary           
                |   'Into Dict'             >> beam.Map(self.map_grouped_records)
                    # write to BigQuery
                |   'WriteData' >> beam.io.gcp.bigquery.WriteToBigQuery(
                                                                        table=self.table_id,
                                                                        schema=queries_schema, 
                                                                        write_disposition='WRITE_APPEND',
                                                                        #method='STREAMING_INSERTS',
                                                                        insert_retry_strategy='RETRY_NEVER',
                                                                        custom_gcs_temp_location=self.temp_gs
                                                                    )
            )


class Driver:

    def __init__(self, bucket_in, location, bucket_temp, local_file, blob_name):
        self.bucket_in      = bucket_in
        self.location       = location
        self.bucket_temp    = bucket_temp
        self.local_file     = local_file
        self.blob_name      = blob_name

    # def __init__(self, bucket_in, location, bucket_temp, local_file, blob_name):

    #     self.bucket_in      = bucket_in
    #     self.location       = location
    #     self.bucket_temp    = bucket_temp
    #     self.local_file     = local_file
    #     self.blob_name      = blob_name


    def prepare_buckets(self):

        storage = GCS()

        print("Creating input bucket.\n")
        storage.delete_bucket(self.bucket_in)
        storage.create_bucket(self.bucket_in, self.location)
        print("Input bucket created.\n")

        print("Uploading file.\n")
        storage.upload_file(self.bucket_in,  self.local_file, self.blob_name)
        print("File uploaded.\n")

        print("Creating temp bucket.\n")
        storage.delete_bucket(self.bucket_temp)
        storage.create_bucket(self.bucket_temp, self.location)
        print("Temp bucket created.\n")


    def setup_airbnb_table(self, table_id):

        # table_id  = 'ricky-311903.assignment.original_airbnb'
        # id,name,host_id,host_name,neighbourhood_group,neighbourhood,latitude,longitude
        # ,room_type,price,minimum_nights,number_of_reviews,last_review,reviews_per_month,
        # calculated_host_listings_count,availability_365

        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("host_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("host_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("neighbourhood_group", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("neighbourhood", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),            
            bigquery.SchemaField("room_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("minimum_nights", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("number_of_reviews", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("last_review", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("reviews_per_month", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("calculated_host_listings_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("availability_365", "INTEGER", mode="NULLABLE"),       
        ]

        bq = GBQ()

        bq.delete(table_id)
        bq.create(table_id, schema)


    def setup_grouped_airbnb_table(self, table_id):

        #table_id  = 'ricky-311903.assignment.grouped_airbnb'

        schema = [
            bigquery.SchemaField("neighbourhood", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("grouped_records", "RECORD", mode="REPEATED",
                fields= [
                    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("host_id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("host_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("neighbourhood_group", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
                    bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),            
                    bigquery.SchemaField("room_type", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("price", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("minimum_nights", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("number_of_reviews", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("last_review", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField("reviews_per_month", "FLOAT64", mode="NULLABLE"),
                    bigquery.SchemaField("calculated_host_listings_count", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("availability_365", "INTEGER", mode="NULLABLE")
                ]
            )     
        ]

        bq = GBQ()

        bq.delete(table_id)
        bq.create(table_id, schema)


def main():
        
    location    = "us-central1"
    bucket_in   = "assignment_input_bucket"
    bucket_temp = "assignment_temp_bucket"
    local_file  = "./AB_NYC_2019.csv"
    blob_name   = "AB_NYC_2019.csv"
    input_file  = f"gs://{bucket_in}/{blob_name}"
    temp_dir    = f"gs://{bucket_in}/"

    table_1_id  = 'ricky-311903.assignment.airbnb'
    table_2_id  = 'ricky-311903.assignment.grouped_airbnb'

    # initialize file and tables
    driver = Driver(bucket_in, location, bucket_temp, local_file, blob_name)
    driver.prepare_buckets()
    driver.setup_airbnb_table(table_1_id)
    driver.setup_grouped_airbnb_table(table_2_id)

    beam_options = PipelineOptions()

    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='ricky-311903',
        job_name='ricky-10301204',
        temp_location=temp_dir,
        region='us-central1'
    )

    #import file into a bigquery table
    etl = Etl(table_1_id, input_file, temp_dir, beam_options)
    etl.run()

    #import file, then group it into a bigquery table
    grouped_etl = Grouped_Etl(table_2_id, input_file, temp_dir, beam_options)
    grouped_etl.run()
  
if __name__=="__main__":
    main()


