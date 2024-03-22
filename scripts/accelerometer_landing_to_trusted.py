import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1711070387038 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity_stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1711070387038",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1711070350511 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity_stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1711070350511",
)

# Script generated for node Join
Join_node1711070412229 = Join.apply(
    frame1=CustomerTrusted_node1711070387038,
    frame2=AccelerometerLanding_node1711070350511,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1711070412229",
)

# Script generated for node Drop columns
SqlQuery879 = """
select user, timestamp, x, y, z from myDataSource

"""
Dropcolumns_node1711073250680 = sparkSqlQuery(
    glueContext,
    query=SqlQuery879,
    mapping={"myDataSource": Join_node1711070412229},
    transformation_ctx="Dropcolumns_node1711073250680",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711070712453 = glueContext.getSink(
    path="s3://udacity-project-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1711070712453",
)
AccelerometerTrusted_node1711070712453.setCatalogInfo(
    catalogDatabase="udacity_stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1711070712453.setFormat("json")
AccelerometerTrusted_node1711070712453.writeFrame(Dropcolumns_node1711073250680)
job.commit()
