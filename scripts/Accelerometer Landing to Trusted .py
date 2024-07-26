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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1721964167754 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://shivaudacityde/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1721964167754")

# Script generated for node Customer Trusted
CustomerTrusted_node1721964168764 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerTrusted_node1721964168764")

# Script generated for node Join
Join_node1721964240971 = Join.apply(frame1=CustomerTrusted_node1721964168764, frame2=AccelerometerLanding_node1721964167754, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1721964240971")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721964286981 = glueContext.getSink(path="s3://shivaudacityde/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1721964286981")
AccelerometerTrusted_node1721964286981.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1721964286981.setFormat("json")
AccelerometerTrusted_node1721964286981.writeFrame(Join_node1721964240971)
job.commit()