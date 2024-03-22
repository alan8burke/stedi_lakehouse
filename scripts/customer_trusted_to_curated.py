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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1711072303671 = glueContext.create_dynamic_frame.from_catalog(database="udacity_stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1711072303671")

# Script generated for node Customer trusted
Customertrusted_node1711071971918 = glueContext.create_dynamic_frame.from_catalog(database="udacity_stedi", table_name="customer_trusted", transformation_ctx="Customertrusted_node1711071971918")

# Script generated for node Join
Join_node1711072394642 = Join.apply(frame1=accelerometer_trusted_node1711072303671, frame2=Customertrusted_node1711071971918, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1711072394642")

# Script generated for node filter empty acceleterometer data
SqlQuery426 = '''
select distinct customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate 
from myDataSource
where myDataSource.timestamp != 0
'''
filteremptyacceleterometerdata_node1711072415113 = sparkSqlQuery(glueContext, query = SqlQuery426, mapping = {"myDataSource":Join_node1711072394642}, transformation_ctx = "filteremptyacceleterometerdata_node1711072415113")

# Script generated for node Customer Curated S3
CustomerCuratedS3_node1711075058846 = glueContext.getSink(path="s3://udacity-project-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedS3_node1711075058846")
CustomerCuratedS3_node1711075058846.setCatalogInfo(catalogDatabase="udacity_stedi",catalogTableName="customers_curated")
CustomerCuratedS3_node1711075058846.setFormat("json")
CustomerCuratedS3_node1711075058846.writeFrame(filteremptyacceleterometerdata_node1711072415113)
job.commit()