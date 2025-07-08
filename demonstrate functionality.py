# Databricks notebook source
# MAGIC %md
# MAGIC **Import functions from other notebooks in the next couple of cells**

# COMMAND ----------

# MAGIC %run /Workspace/Shared/metadata/metadata_fns

# COMMAND ----------

# MAGIC %run /Workspace/Shared/data/data_fns

# COMMAND ----------

# MAGIC %md
# MAGIC **Set the total number of records that you want to generate and number of files, as well as the location to save these files to before running the next cell**

# COMMAND ----------

# DBTITLE 1,generate and write some data for ingestion
total_number_of_records = 5
total_number_of_files = 1
output_location = '/Volumes/main/default/my_volume/user_csv/'

cust_data_df = generate_customer_data(p_number_of_records=total_number_of_records)
fn_write_csv(p_df = cust_data_df,p_num_partitions = total_number_of_files,p_location = output_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the next cell if you make changes to your .yaml files**

# COMMAND ----------

#Im working on  a serverless cluster with no access to dbutils.workspace, otherwise we could recursively scan the workspace to pick up 
#all yaml files automatically. we can still do this using the rest api but its a bit more involved and not somewhere i want to go in this
#demo, so we will hardcode the yaml files in here individually and call load_metadata_to_config_table for each one

load_metadata_to_config_table('/Workspace/Shared/metadata/cdc_project/customer/customer.yml')
spark.table('main.default.metadata').display()

# COMMAND ----------

# DBTITLE 1,write each stream to a delta table
df = spark.sql('select distinct table from main.default.metadata')

for row in df.collect():
    fn_write_stream(row[0])

# COMMAND ----------

# DBTITLE 1,take a look at the destination table
# MAGIC %sql
# MAGIC select * from customer;
# MAGIC
