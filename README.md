Small project to create a framework for ingesting small csv files delivered at infrequent intervals throughout the day using databricks streaming functionality.

Multiple tables are supported, create a yaml file for each and upload the contents to a config table using load_metadata_to_config_table function

Data can be generated using generate_customer_data function after which it can be written to a databricks volume using fn_write_csv

The streams are written to delta table using fn_write_stream

Plenty of scope for improvements in functionality such as wider variety of source formats (eg json, tsv, xls), allow schema drift
