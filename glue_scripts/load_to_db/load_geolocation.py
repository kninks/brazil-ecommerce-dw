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
csv_geo = "s3_cleaned_geolocation_dataset_csv"
db_geo = "geolocation"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"

# --- CSV column names ------------------
zip_code_csv = "geolocation_zip_code_prefix"
city_csv = "geolocation_city"
state_csv = "geolocation_state"
latitude_csv = "geolocation_lat"
longitude_csv = "geolocation_lng"

# --- DB column names -----------------
zip_code_db = "geolocation_zip_code_prefix"
city_db = "geolocation_city"
state_db = "geolocation_state"
latitude_db = "geolocation_lat"
longitude_db = "geolocation_lng"

geo_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_geo
).toDF()

final_df = (
    geo_df
    .withColumn(zip_code_db, lpad(col(zip_code_csv).cast("string"), 5, "0"))
    .withColumn(city_db, col(city_csv).cast("string"))
    .withColumn(state_db, col(state_csv).cast("string"))
    .withColumn(latitude_db, col(latitude_csv).cast("double"))
    .withColumn(longitude_db, col(longitude_csv).cast("double"))
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_geo,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_geo}"
    }
)

job.commit()