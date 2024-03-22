import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step trainer Trusted
SteptrainerTrusted_node1711079104878 = glueContext.create_dynamic_frame.from_catalog(database="udacity_stedi", table_name="step_trainer_trusted", transformation_ctx="SteptrainerTrusted_node1711079104878")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711079175788 = glueContext.create_dynamic_frame.from_catalog(database="udacity_stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1711079175788")

# Script generated for node Join
Join_node1711079288480 = Join.apply(frame1=AccelerometerTrusted_node1711079175788, frame2=SteptrainerTrusted_node1711079104878, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1711079288480")

# Script generated for node ML curated
MLcurated_node1711079465546 = glueContext.getSink(path="s3://udacity-project-stedi/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MLcurated_node1711079465546")
MLcurated_node1711079465546.setCatalogInfo(catalogDatabase="udacity_stedi",catalogTableName="machine_learning_curated")
MLcurated_node1711079465546.setFormat("json")
MLcurated_node1711079465546.writeFrame(Join_node1711079288480)
job.commit()