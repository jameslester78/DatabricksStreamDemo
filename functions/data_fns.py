# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import uuid
from faker import Faker
from pyspark.sql.functions import current_timestamp

def generate_customer_data(p_number_of_records):

    def fake_first_name():
        fake = Faker('en_GB')   # new instance per call
        return fake.first_name()

    def fake_last_name():
        fake = Faker('en_GB')
        return fake.last_name()

    def fake_email():
        fake = Faker('en_GB')
        return fake.ascii_company_email()

    def fake_date():
        fake = Faker('en_GB')
        return fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S")

    def fake_address():
        fake = Faker('en_GB')
        return fake.address()

    def fake_gender():
        fake = Faker('en_GB')
        return fake.profile()['sex']

    def fake_id():
        return str(uuid.uuid4())

    fake_firstname_udf = udf(fake_first_name, StringType())
    fake_lastname_udf = udf(fake_last_name, StringType())
    fake_email_udf = udf(fake_email, StringType())
    fake_date_udf = udf(fake_date, StringType())
    fake_address_udf = udf(fake_address, StringType())
    fake_gender_udf = udf(fake_gender, StringType())
    fake_id_udf = udf(fake_id, StringType())

    df = spark.range(1, p_number_of_records+1)
    df = df.withColumn("creation_date", fake_date_udf())
    df = df.withColumn("firstname", fake_firstname_udf())
    df = df.withColumn("lastname", fake_lastname_udf())
    df = df.withColumn("email", fake_email_udf())
    df = df.withColumn("address", fake_address_udf())
    df = df.withColumn("gender", fake_gender_udf())
    df = df.withColumn("id", fake_id_udf())

    return df


def fn_write_stream(p_table):

    meta_df = spark.sql(f'select key,value from main.default.metadata where table = "{p_table}"')

    metadata = {}
    for row in meta_df.collect():
        metadata[row['key']] = row['value']

    if metadata: #if dictionary is not empty      

        schema = metadata['schema']
        options = metadata['options']
        source = metadata['source']

        schema = eval(schema) #turn string into struct
        options = eval(options) #turn options string into dictionary

        stream_df = (spark.readStream
                        .format("cloudFiles")
                        .options(**options) 
                        .schema(schema)
                        .load(source)
                        )
                    
        from pyspark.sql.functions import col, lit, coalesce
        stream_df = stream_df.withColumn("source_csv_file", coalesce(col("_metadata.file_path"), lit("unknown")))
        stream_df = stream_df.withColumn("ingest_time", current_timestamp())

        checkpointLocation = "/Volumes/main/default/my_volume/checkpoint/" + p_table

        stream_df.writeStream \
            .format("delta") \
            .option("header", "true") \
            .option("checkpointLocation", checkpointLocation) \
            .trigger(availableNow=True) \
            .toTable(p_table)

    else:
        print (f'no metadata found for {p_table}')


    #.start("/Volumes/main/default/my_volume/delta/")\    

# COMMAND ----------

def fn_write_csv(p_df,p_num_partitions,p_location):
    p_df.repartition(p_num_partitions).write.mode("append").format("csv").option("header", "true").save(p_location)
