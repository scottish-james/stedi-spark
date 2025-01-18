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

# Script generated for node cust_trusted
cust_trusted_node1737220719410 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="cust_trusted_node1737220719410")

# Script generated for node acc_trusted
acc_trusted_node1737220736455 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="acc_trusted_node1737220736455")

# Script generated for node join_email
SqlQuery0 = '''
select cust.*, acc.serialnumber_acc from cust
INNER JOIN acc
ON cust.email = acc.user;
'''
join_email_node1737225827703 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cust":cust_trusted_node1737220719410, "acc":acc_trusted_node1737220736455}, transformation_ctx = "join_email_node1737225827703")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=join_email_node1737225827703, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737216380059", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737220830657 = glueContext.getSink(path="s3://my-stedi-bucket-123456/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737220830657")
AmazonS3_node1737220830657.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1737220830657.setFormat("json")
AmazonS3_node1737220830657.writeFrame(join_email_node1737225827703)
job.commit()
