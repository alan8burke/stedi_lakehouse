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

# Script generated for node Customer Landing S3
CustomerLandingS3_node1711069232127 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity_stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerLandingS3_node1711069232127",
)

# Script generated for node Filter Research Opt Out
SqlQuery875 = """
select * from myDataSource
where myDataSource.shareWithResearchAsOfDate != 0
"""
FilterResearchOptOut_node1711069290427 = sparkSqlQuery(
    glueContext,
    query=SqlQuery875,
    mapping={"myDataSource": CustomerLandingS3_node1711069232127},
    transformation_ctx="FilterResearchOptOut_node1711069290427",
)

# Script generated for node Customer Trusted S3
CustomerTrustedS3_node1711069821296 = glueContext.getSink(
    path="s3://udacity-project-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedS3_node1711069821296",
)
CustomerTrustedS3_node1711069821296.setCatalogInfo(
    catalogDatabase="udacity_stedi", catalogTableName="customer_trusted"
)
CustomerTrustedS3_node1711069821296.setFormat("json")
CustomerTrustedS3_node1711069821296.writeFrame(FilterResearchOptOut_node1711069290427)
job.commit()
