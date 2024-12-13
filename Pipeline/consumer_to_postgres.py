import sys
import os
import logging
from configparser import ConfigParser


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Project custom Libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Connections.connection import  spark_consumer_to_df,spark_write_data_to_postgres,spark_read_data_from_postgres

# Load database configuration
config = ConfigParser()
config.read("/home/developer/projects/spark-course-python/spark_course_python/final_project/final_project_CDE_prices/config/config.conf")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def spark_apply_scd_type2(spark:SparkSession ,df_old,df_new):

    target = df_old.withColumnRenamed("itemprice", "itemprice_o")
    source = df_new.withColumnRenamed("itemprice", "itemprice_n")

    target_alias = target.alias("o")
    source_alias = source.alias("n")

    df_new_update = source_alias.join(target_alias.filter(F.col("o.IsActive")==1),
                                ["itemcode","itemname","reshet_num","snif_num"],
                                "outer") \
        .withColumn("Action",
                    F.when(F.col("o.itemprice_o").isNull(), F.lit("NEW"))
                    .when(F.col("n.itemprice_n").isNull(), F.lit("OLD"))
                    .when(F.col("n.itemprice_n") != F.col("o.itemprice_o"), F.lit("UPDATE"))
                    .otherwise(F.lit("NO_CHANGE")))\
        .withColumn("StartDate",
                    F.when(F.col("Action")=="NEW",F.col("n.filedate"))
                    .otherwise(F.col("o.StartDate")))\
        .withColumn("EndDate",
                    F.when(F.col("Action")=="UPDATE",F.col("n.filedate"))
                    .otherwise(F.col("o.EndDate")))\
        .withColumn("IsActive",
                    F.when(F.col("Action")=="NEW",F.lit(1))
                    .when(F.col("Action")=="UPDATE",F.lit(0))
                    .otherwise(F.col("o.IsActive"))) \
        .withColumn("itemprice",
                    F.when(F.col("Action")=="NEW",F.col("n.itemprice_n"))
                    .otherwise(F.col("o.itemprice_o")))  

    df_new_update_change_col = df_new_update\
        .select(F.col("itemcode"),F.col("itemname"),F.col("reshet_num"),F.col("snif_num"),F.col("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice"))


                
    df_add_row = df_new_update.filter(F.col("Action") == "UPDATE")\
        .withColumn("IsActive",F.lit(1))\
        .withColumn("EndDate",F.lit(None))\
        .select(F.col("itemcode"),F.col("itemname"),F.col("reshet_num"),F.col("snif_num"),F.col("filedate").alias("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice_n").alias("itemprice"))


    df_not_active = target.filter(F.col("IsActive")==0)\
        .select(F.col("itemcode"),F.col("itemname"),F.col("reshet_num"),F.col("snif_num"),F.col("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice_o").alias("itemprice"))


    df_final = df_new_update_change_col.union(df_add_row).union(df_not_active)

    return df_final


topic= 'prices'

prices_schema = T.StructType([T.StructField('priceupdatedate',T.StringType(),True),
                            T.StructField('itemcode',T.StringType(),True),
                            T.StructField('itemtype',T.StringType(),True),
                            T.StructField('itemname',T.StringType(),True),
                            T.StructField('manufacturername',T.StringType(),True),
                            T.StructField('manufacturecountry',T.StringType(),True),
                            T.StructField('manufactureritemdescription',T.StringType(),True),
                            T.StructField('unitqty',T.StringType(),True),
                            T.StructField('quantity',T.StringType(),True),
                            T.StructField('unitofmeasure',T.StringType(),True),
                            T.StructField('bisweighted',T.StringType(),True),
                            T.StructField('qtyinpackage',T.StringType(),True),
                            T.StructField('itemprice',T.StringType(),True),
                            T.StructField('unitofmeasureprice',T.StringType(),True),
                            T.StructField('allowdiscount',T.StringType(),True),
                            T.StructField('itemstatus',T.StringType(),True),
                            T.StructField('itemid',T.StringType(),True),
                            T.StructField('file_name',T.StringType(),True),
                            T.StructField('num_reshet',T.StringType(),True),
                            T.StructField('num_snif',T.StringType(),True),
                            T.StructField('file_date',T.TimestampType(),True),
                            T.StructField('run_time',T.TimestampType(),True)
                            ])


spark = SparkSession \
.builder \
.master("local") \
.appName('consumer_prices') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
.getOrCreate()





df_old = spark_read_data_from_postgres(spark, 'dwh.prices_scd')



#load new data from kafka
df_new = spark_consumer_to_df(spark,topic,prices_schema)

#fiter just files "PriceFull"
df_new_full = df_new.filter(F.col("file_name").startswith("PriceFull"))\
                    .withColumn("file_date2",F.to_date(F.col("file_date")))

#add row_number column that give item per reshet,snif,day
window_spec = Window.partitionBy("itemcode", "num_reshet","numsnif","file_date2").orderBy(F.col("file_date").desc())
df_with_row_number = df_new_full.withColumn("rn", F.row_number().over(window_spec))

#filter the max item according to date and max time
df_wo_duplicates_per_day = df_with_row_number.filter(F.col("rn")==1)


df_wo_duplicates_per_day_conv = df_wo_duplicates_per_day.select(
    F.col('itemcode').cast('bigint')\
    ,F.col('itemname').cast('int')\
    ,F.col('itemprice').cast('float')\
    ,F.col('file_date2').cast('date').alias('file_date')\
    ,F.col('reshet_num').cast('int')\
    ,F.col('snif_num').cast('int')
        )

#date uniqe in new data
dates = sorted(df_wo_duplicates_per_day_conv.select("file_date").distinct().rdd.flatMap(lambda x: x).collect())

#loop each date and doing scd between date and old data.
for date in dates:

    df_filtered_day = df_wo_duplicates_per_day_conv.filter(F.col("file_date") == date)

    df_result = spark_apply_scd_type2(spark,df_old, df_filtered_day)

    df_old = df_result



spark.stop()































