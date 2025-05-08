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
csv_order_items = "s3_cleaned_order_items_dataset_csv"
db_order_items = "order_items"
catalog_db_orders = "rds_db_orders"
catalog_db_products = "rds_db_products"
catalog_db_sellers = "rds_db_sellers"

catalog_db = "crawler_db"
rds_connection_name = "project1-jdbc-connection"
rds_db = "DB"


# --- CSV column names ------------------
order_id_csv = "order_id"
order_item_id_csv = "order_item_id"
product_id_csv = "product_id"
seller_id_csv = "seller_id"
shipping_limit_date_csv = "shipping_limit_date"
price_csv = "price"
freight_value_csv = "freight_value"


# --- DB column names -----------------
order_id_db = "order_id"
order_item_id_db = "order_item_id"
product_id_db = "product_id"
seller_id_db = "seller_id"
shipping_limit_date_db = "shipping_limit_date"
price_db = "price"
freight_value_db = "freight_value"


# --- Load data --------------------
order_items_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=csv_order_items
).toDF()
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_orders
).toDF()
products_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_products
).toDF()
sellers_df = glueContext.create_dynamic_frame.from_catalog(
    database=catalog_db,
    table_name=catalog_db_sellers
).toDF()

final_df = (
    order_items_df
    .dropDuplicates([order_id_csv, order_item_id_csv])
    .join(orders_df, order_items_df[order_id_csv] == orders_df[order_id_db], "inner")
    .join(products_df, order_items_df[product_id_csv] == products_df[product_id_db], "inner")
    .join(sellers_df, order_items_df[seller_id_csv] == sellers_df[seller_id_db], "inner")
    .drop(orders_df[order_id_db])
    .drop(products_df[product_id_db])
    .drop(sellers_df[seller_id_db])
    .withColumn(order_id_db, order_items_df[order_id_csv].cast("string"))
    .withColumn(order_item_id_db, order_items_df[order_item_id_csv].cast("integer"))
    .withColumn(product_id_db, order_items_df[product_id_csv].cast("string"))
    .withColumn(seller_id_db, order_items_df[seller_id_csv].cast("string"))
    .withColumn(shipping_limit_date_db, order_items_df[shipping_limit_date_csv].cast("timestamp"))
    .withColumn(price_db, order_items_df[price_csv].cast("float"))
    .withColumn(freight_value_db, order_items_df[freight_value_csv].cast("float"))
    .select(
        order_id_db,
        order_item_id_db,
        product_id_db,
        seller_id_db,
        shipping_limit_date_db,
        price_db,
        freight_value_db
    )
)


# --- Write to RDS ---------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(final_df, glueContext, "final_dyf"),
    catalog_connection=rds_connection_name,
    connection_options={
        "dbtable": db_order_items,
        "database": rds_db,
        "preactions": f"DELETE FROM {db_order_items}"
    }
)

job.commit()