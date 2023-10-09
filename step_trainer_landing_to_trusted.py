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

# Script generated for node Customers Curated
CustomersCurated_node1696869963939 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1696869963939",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://rks-stedi-human-balance-analytics/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696870107745 = ApplyMapping.apply(
    frame=CustomersCurated_node1696869963939,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696870107745",
)

# Script generated for node Join
Join_node1696869949156 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforJoin_node1696870107745,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696869949156",
)

# Script generated for node Drop Fields
DropFields_node1696870041090 = DropFields.apply(
    frame=Join_node1696869949156,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1696870041090",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696870041090,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://rks-stedi-human-balance-analytics/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node2",
)

job.commit()
