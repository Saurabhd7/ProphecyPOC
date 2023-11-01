from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from expression_builder.config.ConfigStore import *
from expression_builder.udfs.UDFs import *

def customer_churn(spark: SparkSession) -> DataFrame:
    return spark.read\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn.csv")
