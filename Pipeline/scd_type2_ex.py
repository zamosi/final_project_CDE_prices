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
from Connections.connection import connect_to_postgres_data,truncate_table_in_postgres



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


    
    try:


        df_new_update = source_alias.join(target_alias.filter(F.col("o.IsActive")==1),
                                    ["itemcode","itemname","num_reshet","num_snif"],
                                    "outer") \
            .withColumn("Action",
                        F.when(F.col("o.itemprice_o").isNull(), F.lit("NEW"))
                        .when(F.col("n.itemprice_n").isNull(), F.lit("OLD"))
                        .when(F.col("n.itemprice_n") != F.col("o.itemprice_o"), F.lit("UPDATE"))
                        .otherwise(F.lit("NO_CHANGE")))\
            .withColumn("StartDate",
                        F.when(F.col("Action")=="NEW",F.col("n.file_date"))
                        .otherwise(F.col("o.StartDate")))\
            .withColumn("EndDate",
                        F.when(F.col("Action")=="UPDATE",F.col("n.file_date"))
                        .otherwise(F.col("o.EndDate")))\
            .withColumn("IsActive",
                        F.when(F.col("Action")=="NEW",F.lit(1))
                        .when(F.col("Action")=="UPDATE",F.lit(0))
                        .otherwise(F.col("o.IsActive"))) \
            .withColumn("itemprice",
                        F.when(F.col("Action")=="NEW",F.col("n.itemprice_n"))
                        .otherwise(F.col("o.itemprice_o")))  
        

        df_new_update_change_col = df_new_update\
            .select(F.col("itemcode"),F.col("itemname"),F.col("num_reshet"),F.col("num_snif"),F.col("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice"))
        
        logger.info("succeeded-scd-pop1")
    except Exception as e:
        logger.error(f"err-scd-pop1 {e}")
        raise


    try:
        df_add_row = df_new_update.filter(F.col("Action") == "UPDATE")\
            .withColumn("IsActive",F.lit(1))\
            .withColumn("EndDate",F.lit(None))\
            .select(F.col("itemcode"),F.col("itemname"),F.col("num_reshet"),F.col("num_snif"),F.col("file_date").alias("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice_n").alias("itemprice"))

        logger.info("succeeded-scd-pop2")
    except Exception as e:
        logger.error(f"err-scd-pop2 {e}")
        raise

    try:
        df_not_active = target.filter(F.col("IsActive")==0)\
            .select(F.col("itemcode"),F.col("itemname"),F.col("num_reshet"),F.col("num_snif"),F.col("StartDate"),F.col("EndDate"),F.col("IsActive"),F.col("itemprice_o").alias("itemprice"))
        
        logger.info("succeeded-scd-pop3")
    except Exception as e:
        logger.error(f"err-scd-pop3 {e}")
        raise


    try:
        df_final = df_new_update_change_col.unionAll(df_add_row).unionAll(df_not_active)

        logger.info("succeeded-scd-union")
    except Exception as e:
        logger.error(f"err-scd-union {e}")
        raise

    return df_final





spark = SparkSession \
.builder \
.master("local[*]") \
.appName('consumer_prices') \
.config("spark.executor.memory", "4g") \
.config("spark.driver.memory", "4g") \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
.config("spark.jars", config["Core_Settings"]["POSTGRES_JDBC_DRIVERS_PATH"])\
.getOrCreate()

schema = T.StructType([
    T.StructField("itemcode", T.LongType(), True),
    T.StructField("itemname", T.StringType(), True),
    T.StructField("itemprice", T.IntegerType(), True),
    T.StructField("StartDate", T.DateType(), True),  # אפשר להחליף ל-DateType אם נדרש
    T.StructField("EndDate", T.DateType(), True),
    T.StructField("IsActive", T.IntegerType(), True),
    T.StructField("num_reshet", T.LongType(), True),
    T.StructField("num_snif", T.IntegerType(), True),
])

df_old = spark.createDataFrame(
    [
        (286549, "מטליות לניקוי כללי", 10, "2024-12-12", None, 1,7290700100008,101),
        (123, "ששש", 10, "2024-12-12", None, 1,7290700100008,101)
        ]
    , schema
["itemcode", "itemname", "itemprice", "StartDate", "EndDate", "IsActive","num_reshet","num_snif"]
)

# df_new = spark_read_data_from_postgres(spark, 'dwh.all_a')

df_new = spark.createDataFrame(
    # [(286549, "מטליות לניקוי כללי", 15, "2024-12-13 00:40:00.000",7290700100008,101),
    #  (286549, "מטליות לניקוי כללי", 17, "2024-12-14 00:40:00.000",7290700100008,101),
    #  (286549, "מטליות לניקוי כללי", 20, "2024-12-15 00:40:00.000",7290700100008,101)
    #  ], 
["itemcode", "itemname", "itemprice", "file_date","num_reshet","num_snif"]
)






#fiter just files "PriceFull"
df_new_full = df_new.filter( (F.col('num_reshet') == '7290700100008')) \
                    .withColumn("file_date2",F.to_date(F.col("file_date")))

#add row_number column that give item per reshet,snif,day
window_spec = Window.partitionBy("itemcode", "num_reshet","num_snif","file_date2").orderBy(F.col("file_date").desc())
df_with_row_number = df_new_full.withColumn("rn", F.row_number().over(window_spec))

#filter the max item according to date and max time
df_wo_duplicates_per_day = df_with_row_number.filter(F.col("rn")==1)


df_wo_duplicates_per_day_conv = df_wo_duplicates_per_day.select(
    F.col('itemcode').cast('bigint')\
    ,F.col('itemname').cast('string')\
    ,F.col('itemprice').cast('float')\
    ,F.col('file_date2').cast('date').alias('file_date')\
    ,F.col('num_reshet').cast('bigint')\
    ,F.col('num_snif').cast('int')
        )

#date uniqe in new data
dates = sorted(df_wo_duplicates_per_day_conv.select("file_date").distinct().rdd.flatMap(lambda x: x).collect())

#loop each date and doing scd between date and old data.
for date in dates:

    df_filtered_day = df_wo_duplicates_per_day_conv.filter(F.col("file_date") == date)

    df_result = spark_apply_scd_type2(spark,df_old, df_filtered_day)

    df_old = df_result

conn, engine = connect_to_postgres_data()


# truncate_table_in_postgres(conn,'dwh.prices_scd')
# spark_write_data_to_postgres(spark,'dwh.prices_scd',df_old)

df_old.show()

spark.stop()































