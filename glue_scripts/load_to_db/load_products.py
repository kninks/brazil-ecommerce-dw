import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from pyspark.sql.functions import col, expr
from awsglue.dynamicframe import DynamicFrame


# --- Glue and Spark jobs config --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Config ---------------------
csv_products = "s3_cleaned_products_dataset_csv"
db_products = "products"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"

# --- CSV column names ------------------
product_id_csv = "product_id"
product_category_csv = "product_category_name"
product_photos_qty_csv = "product_photos_qty"
product_length_cm_csv = "product_length_cm"
product_height_cm_csv = "product_height_cm"
product_width_cm_csv = "product_width_cm"
product_weight_g_csv = "product_weight_g"

# --- DB column names -----------------
product_id_db = "product_id"
product_category_db = "product_category_name"
product_photos_qty_db = "product_photos_qty"
product_length_cm_db = "product_length_cm"
product_height_cm_db = "product_height_cm"
product_width_cm_db = "product_width_cm"
product_weight_g_db = "product_weight_g"


# --- Load data --------------------
cust_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_products
).toDF()

final_df = (
    cust_df
    .withColumn(product_id_db, col(product_id_csv).cast("string"))
    .withColumn(product_category_db, col(product_category_csv).cast("string"))
    .withColumn(product_photos_qty_db, col(product_photos_qty_csv).cast("integer"))
    .withColumn(product_weight_g_db, col(product_weight_g_csv).cast("double"))
    .withColumn(product_length_cm_db, col(product_length_cm_csv).cast("double"))
    .withColumn(product_height_cm_db, col(product_height_cm_csv).cast("double"))
    .withColumn(product_width_cm_db, col(product_width_cm_csv).cast("double"))

)


# --- Write to RDS ---------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_products,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_products}"
    }
)

job.commit()