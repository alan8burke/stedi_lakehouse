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

# Script generated for node Step trainer Landing
SteptrainerLanding_node1711076055095 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity_stedi",
    table_name="step_trainer_landing",
    transformation_ctx="SteptrainerLanding_node1711076055095",
)

# Script generated for node Customers Curated
CustomersCurated_node1711076286636 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity_stedi",
    table_name="customers_curated",
    transformation_ctx="CustomersCurated_node1711076286636",
)

# Script generated for node SQL Query
SqlQuery1026 = """
select stl.sensorreadingtime, stl.serialnumber, stl.distancefromobject 
from stl
join cc on stl.serialnumber = cc.serialnumber
"""
SQLQuery_node1711076455959 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1026,
    mapping={
        "stl": SteptrainerLanding_node1711076055095,
        "cc": CustomersCurated_node1711076286636,
    },
    transformation_ctx="SQLQuery_node1711076455959",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1711078253448 = glueContext.getSink(
    path="s3://udacity-project-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Steptrainertrusted_node1711078253448",
)
Steptrainertrusted_node1711078253448.setCatalogInfo(
    catalogDatabase="udacity_stedi", catalogTableName="step_trainer_trusted"
)
Steptrainertrusted_node1711078253448.setFormat("json")
Steptrainertrusted_node1711078253448.writeFrame(SQLQuery_node1711076455959)
job.commit()
