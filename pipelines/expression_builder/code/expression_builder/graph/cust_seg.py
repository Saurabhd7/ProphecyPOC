from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from expression_builder.config.ConfigStore import *
from expression_builder.udfs.UDFs import *

def cust_seg(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "cust_segment",
        when((col("TotalCharges") <= lit(2000)), lit("Low"))\
          .when(((col("TotalCharges") >= lit(2000)) & (col("TotalCharges") <= lit(5000))), lit("Med"))\
          .otherwise(lit("High"))
    )
