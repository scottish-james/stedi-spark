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

# Script generated for node Customer Landing
CustomerLanding_node1737150415442 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1737150415442")

# Script generated for node Private Filter 
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate IS NOT NULL;
'''
PrivateFilter_node1737150373034 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1737150415442}, transformation_ctx = "PrivateFilter_node1737150373034")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=PrivateFilter_node1737150373034, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737149227558", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737150516293 = glueContext.getSink(path="s3://my-stedi-bucket-123456/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737150516293")
AmazonS3_node1737150516293.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1737150516293.setFormat("json")
AmazonS3_node1737150516293.writeFrame(PrivateFilter_node1737150373034)
job.commit()
