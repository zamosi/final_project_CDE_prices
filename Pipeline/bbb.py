from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# יצירת SparkSession
spark = SparkSession.builder.master("local").appName("SCD Type 2").getOrCreate()

# דוגמת טבלת target עם נתונים היסטוריים
df_old = spark.createDataFrame(
    [
    (1, "Product A", 10, "2024-01-01", "2024-02-01", 0,1,1),
    (1, "Product A", 15, "2024-02-01", None, 1,1,1),
    (2, "Product B", 20, "2024-01-01", None, 1,1,1),
    (4, "Product D", 30, "2024-01-01", None, 1,1,1)
], 
["itemcode", "itemname", "itemprice", "StartDate", "EndDate", "IsActive","reshet_num","snif_num"]
)

# דוגמת טבלת source עם נתונים חדשים
df_new = spark.createDataFrame([
    (1, "Product A", 15, "2024-02-10",1,1),
    (2, "Product B", 25, "2024-02-10",1,1),
    (3, "Product C", 30, "2024-02-10",1,1)
], 
["itemcode", "itemname", "itemprice", "filedate","reshet_num","snif_num"]
)


def apply_scd_type2(spark:SparkSession ,df_old,df_new):

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

    df_final.show()


apply_scd_type2(spark,df_old,df_new)