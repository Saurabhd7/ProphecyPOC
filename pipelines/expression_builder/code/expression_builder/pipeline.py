from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from expression_builder.config.ConfigStore import *
from expression_builder.udfs.UDFs import *
from prophecy.utils import *
from expression_builder.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customer_churn = customer_churn(spark)
    df_cust_seg = cust_seg(spark, df_customer_churn)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/expression_builder")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/expression_builder", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/expression_builder")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
