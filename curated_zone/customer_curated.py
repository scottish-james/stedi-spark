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

# Script generated for node accelerometer trusted
accelerometertrusted_node983247982374 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://my-stedi-bucket-123456/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node983247982374",
)

# Script generated for node customer trusted
customertrusted_node1234567890123 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://my-stedi-bucket-123456/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1234567890123",
)

# Script generated for node Join
Join_node2837465928471 = Join.apply(
    frame1=accelerometertrusted_node983247982374,
    frame2=customertrusted_node1234567890123,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node2837465928471",
)

# Script generated for node Drop Fields
DropFields_node3746598273405 = DropFields.apply(
    frame=Join_node2837465928471,
    paths=["x", "y", "user", "timeStamp", "z"],
    transformation_ctx="DropFields_node3746598273405",
)

# Script generated for node Drop Duplicates
DropDuplicates_node6598273485730 = DynamicFrame.fromDF(
    DropFields_node3746598273405.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node6598273485730",
)

# Script generated for node customer curated
customercurated_node9823746592387 = glueContext.getSink(
    path="s3://my-stedi-bucket-123456/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node9823746592387",
)
customercurated_node9823746592387.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
customercurated_node9823746592387.setFormat("json")
customercurated_node9823746592387.writeFrame(DropDuplicates_node6598273485730)
job.commit()
