import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from pyspark.sql.functions import col, expr, lpad
from awsglue.dynamicframe import DynamicFrame


# --- Glue and Spark jobs config --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# --- Config ---------------------
csv_cust = "s3_cleaned_customers_dataset_csv"
db_cust = "customers"
catalog_db_geo = "rds_db_geolocation"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"


# --- CSV column names ------------------
customer_id_csv = "customer_id"
customer_unique_id_csv = "customer_unique_id"
zip_code_csv = "customer_zip_code_prefix"
city_csv = "customer_city"
state_csv = "customer_state"


# --- DB column names -----------------
customer_id_db = "customer_id"
customer_unique_id_db = "customer_unique_id"
zip_code_db = "customer_zip_code_prefix"
city_db = "customer_city"
state_db = "customer_state"

# geo db
geo_zip_code_db = "geolocation_zip_code_prefix"


# --- Load data --------------------
geo_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_geo
).toDF()
cust_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_cust
).toDF()

geo_df = (
    geo_df
    .withColumn(geo_zip_code_db, lpad(col(geo_zip_code_db).cast("string"), 5, "0"))
)
cust_df = (
    cust_df
    .withColumn(zip_code_csv, lpad(col(zip_code_csv).cast("string"), 5, "0"))
)

final_df = (
    cust_df
    .dropDuplicates([customer_id_csv])
    .join(geo_df, cust_df[zip_code_csv] == geo_df[geo_zip_code_db], "inner")
    .withColumn(customer_id_db, col(customer_id_csv).cast("string"))
    .withColumn(customer_unique_id_db, col(customer_unique_id_csv).cast("string"))
    .withColumn(zip_code_db, col(zip_code_csv))
    .withColumn(city_db, col(city_csv).cast("string"))
    .withColumn(state_db, col(state_csv).cast("string"))
    .select(customer_id_db, customer_unique_id_db, zip_code_db, city_db, state_db)
)


# --- Write to RDS ---------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_cust,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_cust}"
    }
)

job.commit()