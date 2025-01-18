import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node acc_trusted
acc_trusted_node1737209309632 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1737209309632")

# Script generated for node step_trainer
step_trainer_node1737209326212 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_node1737209326212")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
    FROM step_trainer_landing
    JOIN accelerometer_trusted
    ON step_trainer_landing.sensorreadingtime = accelerometer_trusted.timestamp
       AND step_trainer_landing.serialnumber = accelerometer_trusted.serialnumber_acc
'''
SQLQuery_node1737217486215 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_node1737209326212, "accelerometer_trusted":acc_trusted_node1737209309632}, transformation_ctx = "SQLQuery_node1737217486215")

# Script generated for node drop duplicates
SqlQuery1 = '''
SELECT DISTINCT sensorreadingtime, serialnumber, istanceFromObject
    FROM step_trainer_landing
'''
dropduplicates_node1737218957476 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"":SQLQuery_node1737217486215}, transformation_ctx = "dropduplicates_node1737218957476")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=dropduplicates_node1737218957476, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737209290193", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737211131986 = glueContext.getSink(path="s3://my-stedi-bucket-123456/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737211131986")
AmazonS3_node1737211131986.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1737211131986.setFormat("json")
AmazonS3_node1737211131986.writeFrame(dropduplicates_node1737218957476)
job.commit()
