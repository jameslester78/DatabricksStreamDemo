# Databricks notebook source
import yaml
from delta.tables import DeltaTable

def load_metadata_to_config_table(p_yaml_path):


    with open(p_yaml_path) as stream:
        y = yaml.safe_load(stream)


    data = []
    for k, v in y["details"].items():
        data.append({
            "table": y["table_name"],
            "key": k,
            "value": str(v)
        })

    df = spark.createDataFrame(data)

    df = df.select("table", "key", "value")

    table_name = 'main.default.metadata'
    table_exists = spark.catalog.tableExists(table_name)

    if not table_exists:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        merge_condition = "target.table = source.table and target.key = source.key"
        delta_table.alias("target").merge(
                source=df.alias("source"),
                condition=merge_condition
            ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .whenNotMatchedBySourceDelete() \
        .execute()
