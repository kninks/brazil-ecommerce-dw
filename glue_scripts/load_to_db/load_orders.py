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
csv_orders = "s3_cleaned_orders_dataset_csv"
db_orders = "orders"
catalog_db_cust = "rds_db_customers"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"


# --- CSV column names ------------------
order_id_csv = "order_id"
customer_id_csv = "customer_id"
order_status_csv = "order_status"
order_purchase_csv = "order_purchase_timestamp"
order_approval_csv = "order_approved_at"
order_delivery_csv = "order_delivered_carrier_date"
order_arrival_csv = "order_delivered_customer_date"
order_estimated_arrival_csv = "order_estimated_delivery_date"


# --- DB column names -----------------
order_id_db = "order_id"
customer_id_db = "customer_id"
order_status_db = "order_status"
order_purchase_db = "order_purchase_timestamp"
order_approval_db = "order_approved_at"
order_delivery_db = "order_delivered_carrier_date"
order_arrival_db = "order_delivered_customer_date"
order_estimated_arrival_db = "order_estimated_delivery_date"


# --- Load data --------------------
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_orders
).toDF()
customers_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_cust
).toDF()

final_df = (
    orders_df
    .dropDuplicates([customer_id_csv])
    .join(customers_df, orders_df[customer_id_csv] == customers_df[customer_id_db], "inner")
    .drop(customers_df[customer_id_db])
    .withColumn(order_id_db, orders_df[order_id_csv].cast("string"))
    .withColumn(customer_id_db, orders_df[customer_id_csv].cast("string"))
    .withColumn(order_status_db, orders_df[order_status_csv].cast("string"))
    .withColumn(order_purchase_db, orders_df[order_purchase_csv].cast("timestamp"))
    .withColumn(order_approval_db, orders_df[order_approval_csv].cast("timestamp"))
    .withColumn(order_delivery_db, orders_df[order_delivery_csv].cast("timestamp"))
    .withColumn(order_arrival_db, orders_df[order_arrival_csv].cast("timestamp"))
    .withColumn(order_estimated_arrival_db, orders_df[order_estimated_arrival_csv].cast("timestamp"))
    .select(
        order_id_db,
        customer_id_db,
        order_status_db,
        order_purchase_db,
        order_approval_db,
        order_delivery_db,
        order_arrival_db,
        order_estimated_arrival_db
    )
)


# --- Write to RDS ---------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_orders,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_orders}"
    }
)

job.commit()