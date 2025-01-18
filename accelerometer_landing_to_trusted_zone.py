import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node acc_landing
acc_landing_node1737203442098 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="acc_landing_node1737203442098")

# Script generated for node cust_trusted
cust_trusted_node1737203424853 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="cust_trusted_node1737203424853")

# Script generated for node Join
Join_node1737203468890 = Join.apply(frame1=acc_landing_node1737203442098, frame2=cust_trusted_node1737203424853, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1737203468890")

# Script generated for node Drop Duplicates
DropDuplicates_node1737203640743 =  DynamicFrame.fromDF(Join_node1737203468890.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1737203640743")

# Script generated for node Change Schema
ChangeSchema_node1737209624115 = ApplyMapping.apply(frame=DropDuplicates_node1737203640743, mappings=[("serialNumber", "string", "serialNumber_acc", "string"), ("z", "double", "z", "float"), ("y", "double", "y", "float"), ("x", "double", "x", "float"), ("timestamp", "long", "timestamp", "long")], transformation_ctx="ChangeSchema_node1737209624115")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1737209624115, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737203399676", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737203648668 = glueContext.getSink(path="s3://my-stedi-bucket-123456/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737203648668")
AmazonS3_node1737203648668.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1737203648668.setFormat("json")
AmazonS3_node1737203648668.writeFrame(ChangeSchema_node1737209624115)
job.commit()
