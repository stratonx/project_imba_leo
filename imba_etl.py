import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper

# --------------------------------------------------------------------------------
# 1. READ JOB PARAMETERS
# --------------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'input_path',
        'output_path'
    ]
)

# --------------------------------------------------------------------------------
# 2. INITIALIZE CONTEXT
# --------------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------------------------------------
# 3. READ DATA FROM S3 (CSV)
# --------------------------------------------------------------------------------
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(args['input_path'])

print("=== Input Schema ===")
df.printSchema()

# --------------------------------------------------------------------------------
# 4. TRANSFORM DATA
# Example: uppercase a column + filter nulls
# --------------------------------------------------------------------------------
transformed_df = df \
    .withColumn("name_upper", upper(col("name"))) \
    .filter(col("name").isNotNull())

# --------------------------------------------------------------------------------
# 5. WRITE TO S3 (PARQUET)
# --------------------------------------------------------------------------------
transformed_df.write \
    .mode("overwrite") \
    .parquet(args['output_path'])

print("=== Job Completed Successfully ===")

# --------------------------------------------------------------------------------
# 6. COMMIT JOB
# --------------------------------------------------------------------------------
job.commit()
