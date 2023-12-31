import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696868860997 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696868860997",
)

# Script generated for node Join Customer
JoinCustomer_node1696868917434 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1696868860997,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1696868917434",
)

# Script generated for node Drop Fields
DropFields_node1696868979057 = DropFields.apply(
    frame=JoinCustomer_node1696868917434,
    paths=["x", "y", "user", "timeStamp", "z"],
    transformation_ctx="DropFields_node1696868979057",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696868985685 = DynamicFrame.fromDF(
    DropFields_node1696868979057.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696868985685",
)

# Script generated for node Customer Curated
CustomerCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696868985685,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://rks-stedi-human-balance-analytics/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node2",
)

job.commit()
