import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1689563329110 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1689563329110",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1689563552518 = Join.apply(
    frame1=accelerometer_landing_node1,
    frame2=customer_trusted_node1689563329110,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1689563552518",
)

# Script generated for node Drop Fields
DropFields_node1689563733947 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1689563552518,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node1689563733947",
)
# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.getSink(
    path="s3://shiva-stedi/customer/curated/customer_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node3",
)
CustomerCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node3.setFormat("json")
CustomerCurated_node3.writeFrame(DropFields_node1685823281966)
job.commit()
