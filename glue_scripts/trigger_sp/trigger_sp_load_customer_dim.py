import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


dummy_df = spark.sql("SELECT 1 AS id")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(dummy_df, glueContext, "dummy"),
    catalog_connection="project1-jdbc-connection",
    connection_options={
        "database": "DW",
        "dbtable": "dummy_sp_runner",
        "postactions": f"CALL DW.load_customer_dim();"
    }
)

job.commit()