import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://shiva-stedi/customer/landing"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1689302643878 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1689302643878",
)


# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1688095478499 = glueContext.getSink(
    path="s3://shiva-stedi/customer/trusted/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedZone_node1688095478499",
)
CustomerTrustedZone_node1688095478499.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrustedZone_node1688095478499.setFormat("json")
CustomerTrustedZone_node1688095478499.writeFrame(Filter_node1688095289055)
job.commit()
