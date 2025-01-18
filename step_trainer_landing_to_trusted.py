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

# Script generated for node acc_trusted
acc_trusted_node1737209309632 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1737209309632")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1737209326212 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1737209326212")

# Script generated for node Join
Join_node1737209339958 = Join.apply(frame1=AWSGlueDataCatalog_node1737209326212, frame2=acc_trusted_node1737209309632, keys1=["serialnumber", "sensorreadingtime"], keys2=["serialnumber_acc", "timestamp"], transformation_ctx="Join_node1737209339958")

# Script generated for node Drop Duplicates
DropDuplicates_node1737211110061 =  DynamicFrame.fromDF(Join_node1737209339958.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1737211110061")

# Script generated for node Change Schema
ChangeSchema_node1737212800116 = ApplyMapping.apply(frame=DropDuplicates_node1737211110061, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1737212800116")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1737212800116, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737209290193", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737211131986 = glueContext.getSink(path="s3://my-stedi-bucket-123456/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737211131986")
AmazonS3_node1737211131986.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1737211131986.setFormat("json")
AmazonS3_node1737211131986.writeFrame(ChangeSchema_node1737212800116)
job.commit()
