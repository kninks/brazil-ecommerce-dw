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
csv_order_payments = "s3_cleaned_order_payments_dataset_csv"
db_order_payments = "order_payments"
catalog_db_orders = "rds_db_orders"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"


# --- CSV column names ------------------
order_id_csv = "order_id"
payment_sequential_csv = "payment_sequential"
payment_type_csv = "payment_type"
payment_installments_csv = "payment_installments"
payment_value_csv = "payment_value"


# --- DB column names -----------------
order_id_db = "order_id"
payment_sequential_db = "payment_sequential"
payment_type_db = "payment_type"
payment_installments_db = "payment_installments"
payment_value_db = "payment_value"


# --- Load data --------------------
order_payments_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_order_payments
).toDF()
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_orders
).toDF()

final_df = (
    order_payments_df
    .dropDuplicates([order_id_csv])
    .join(orders_df, order_payments_df[order_id_csv] == orders_df[order_id_db], "inner")
    .drop(orders_df[order_id_db])
    .withColumn(order_id_db, order_payments_df[order_id_csv].cast("string"))
    .withColumn(payment_sequential_db, order_payments_df[payment_sequential_csv].cast("integer"))
    .withColumn(payment_type_db, order_payments_df[payment_type_csv].cast("string"))
    .withColumn(payment_installments_db, order_payments_df[payment_installments_csv].cast("integer"))
    .withColumn(payment_value_db, order_payments_df[payment_value_csv].cast("float"))
    .select(
        order_id_db,
        payment_sequential_db,
        payment_type_db,
        payment_installments_db,
        payment_value_db
    )
)


# --- Write to RDS ---------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_order_payments,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_order_payments}"
    }
)

job.commit()