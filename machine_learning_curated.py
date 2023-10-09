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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Customers Curated
CustomersCurated_node1696870929539 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1696870929539",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696870740070 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696870740070",
)

# Script generated for node Join Accelerometer
JoinAccelerometer_node1696870862156 = Join.apply(
    frame1=AccelerometerTrusted_node1696870740070,
    frame2=StepTrainerTrusted_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinAccelerometer_node1696870862156",
)

# Script generated for node Join
Join_node1696871016780 = Join.apply(
    frame1=CustomersCurated_node1696870929539,
    frame2=JoinAccelerometer_node1696870862156,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1696871016780",
)

# Script generated for node Drop Fields
DropFields_node1696871260562 = DropFields.apply(
    frame=Join_node1696871016780,
    paths=[
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1696871260562",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696871260562,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://rks-stedi-human-balance-analytics/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
