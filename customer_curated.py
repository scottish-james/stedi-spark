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

# Script generated for node cust_trusted
cust_trusted_node1737220719410 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="cust_trusted_node1737220719410")

# Script generated for node acc_trusted
acc_trusted_node1737220736455 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1737220736455")

# Script generated for node Join
Join_node1737221908126 = Join.apply(frame1=cust_trusted_node1737220719410, frame2=acc_trusted_node1737220736455, keys1=["serialnumber"], keys2=["serialnumber_acc"], transformation_ctx="Join_node1737221908126")

# Script generated for node Change Schema
ChangeSchema_node1737222735788 = ApplyMapping.apply(frame=Join_node1737221908126, mappings=[("serialNumber", "string", "serialNumber", "string"), ("birthDay", "string", "birthDay", "string"), ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"), ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "registrationDate", "long"), ("customerName", "string", "customerName", "string"), ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"), ("email", "string", "email", "string"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1737222735788")

# Script generated for node Drop Duplicates
DropDuplicates_node1737222275381 =  DynamicFrame.fromDF(ChangeSchema_node1737222735788.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1737222275381")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1737222275381, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737216380059", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737220830657 = glueContext.getSink(path="s3://my-stedi-bucket-123456/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737220830657")
AmazonS3_node1737220830657.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1737220830657.setFormat("json")
AmazonS3_node1737220830657.writeFrame(DropDuplicates_node1737222275381)
job.commit()
