import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Stept trainer (trusted zone)
Stepttrainertrustedzone_node4387659823471 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://my-stedi-bucket-123456/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Stepttrainertrustedzone_node4387659823471",
)

# Script generated for node Accelerometer (trusted)
Accelerometertrusted_node7283645102938 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://my-stedi-bucket-123456/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometertrusted_node7283645102938",
)

# Script generated for node Curated customers data
Curatedcustomersdata_node9248571039486 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://my-stedi-bucket-123456/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Curatedcustomersdata_node9248571039486",
)

# Script generated for node Join
Join_node2837495102938 = Join.apply(
    frame1=Accelerometertrusted_node7283645102938,
    frame2=Curatedcustomersdata_node9248571039486,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node2837495102938",
)

# Script generated for node Join
Join_node8472635092837 = Join.apply(
    frame1=Join_node2837495102938,
    frame2=Stepttrainertrustedzone_node4387659823471,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node8472635092837",
)

# Script generated for node Aggregate
Aggregate_node9874650928374 = sparkAggregate(
    glueContext,
    parentFrame=Join_node8472635092837,
    groups=[
        "y",
        "z",
        "x",
        "user",
        "sensorReadingTime",
        "distanceFromObject",
        "serialNumber",
    ],
    aggs=[["timeStamp", "count"]],
    transformation_ctx="Aggregate_node9874650928374",
)

# Script generated for node Machine Learning data (curated)
MachineLearningdatacurated_node2938476509184 = glueContext.getSink(
    path="s3://my-stedi-bucket-123456/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningdatacurated_node2938476509184",
)
MachineLearningdatacurated_node2938476509184.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningdatacurated_node2938476509184.setFormat("json")
MachineLearningdatacurated_node2938476509184.writeFrame(Aggregate_node9874650928374)
job.commit()
