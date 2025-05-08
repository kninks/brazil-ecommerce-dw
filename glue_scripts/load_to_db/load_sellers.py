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
csv_seller = "s3_cleaned_sellers_dataset_csv"
db_seller = "sellers"
catalog_db_geo = "rds_db_geolocation"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"


# --- CSV column names ------------------
seller_id_csv = "seller_id"
zip_code_csv = "seller_zip_code_prefix"
city_csv = "seller_city"
state_csv = "seller_state"


# --- DB column names -----------------
seller_id_db = "seller_id"
zip_code_db = "seller_zip_code_prefix"
city_db = "seller_city"
state_db = "seller_state"

# geo db
geo_zip_code_db = "geolocation_zip_code_prefix"


# --- Load data --------------------
geo_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_geo
).toDF()
seller_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_seller
).toDF()

geo_df = (
    geo_df
    .withColumn(geo_zip_code_db, lpad(col(geo_zip_code_db).cast("string"), 5, "0"))
)
seller_df = (
    seller_df
    .withColumn(zip_code_csv, lpad(col(zip_code_csv).cast("string"), 5, "0"))
)

final_df = (
    seller_df
    .dropDuplicates([seller_id_csv])
    .join(geo_df, seller_df[zip_code_csv] == geo_df[geo_zip_code_db], "inner")
    .withColumn(seller_id_db, col(seller_id_csv).cast("string"))
    .withColumn(zip_code_db, col(zip_code_csv))
    .withColumn(city_db, col(city_csv).cast("string"))
    .withColumn(state_db, col(state_csv).cast("string"))
    .select(seller_id_db, zip_code_db, city_db, state_db)
)


# --- Write to RDS --------------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_seller,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_seller}"
    }
)

job.commit()