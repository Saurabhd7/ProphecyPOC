from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from expression_builder.config.ConfigStore import *
from expression_builder.udfs.UDFs import *

def cust_segm(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("append")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/FileStore/tables/output/customer_segment.csv")
