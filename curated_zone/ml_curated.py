import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node step_trusted
step_trusted_node1737223026803 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trusted", transformation_ctx="step_trusted_node1737223026803")

# Script generated for node acc_trusted
acc_trusted_node1737223049372 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1737223049372")

# Script generated for node SQL JOIN
SqlQuery0 = '''
select * from step
INNER JOIN acc
ON step.sensorreadingtime = acc.timestamp;
'''
SQLJOIN_node1737224026442 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"acc":acc_trusted_node1737223049372, "step":step_trusted_node1737223026803}, transformation_ctx = "SQLJOIN_node1737224026442")

# Script generated for node Drop Duplicates
DropDuplicates_node1737224290420 =  DynamicFrame.fromDF(SQLJOIN_node1737224026442.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1737224290420")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1737224290420, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737222937604", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737223103843 = glueContext.getSink(path="s3://my-stedi-bucket-123456/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737223103843")
AmazonS3_node1737223103843.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_curated")
AmazonS3_node1737223103843.setFormat("json")
AmazonS3_node1737223103843.writeFrame(DropDuplicates_node1737224290420)
job.commit()
