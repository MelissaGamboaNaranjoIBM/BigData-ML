import sys
import os
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions import crear_df_farms, crear_df_productivity, crear_df_agronomic,crear_df_sensors


spark = SparkSession.builder \
    .appName("Proyecto Final - PostgreSQL Write") \
    .config("spark.jars", "postgresql-42.2.14.jar") \
    .getOrCreate()


input_1 = sys.argv[1]  
input_2 = sys.argv[2]  
input_3 = sys.argv[3]  
input_4 = sys.argv[4] 

df_farms = crear_df_farms(input_1, spark)
df_productivity = crear_df_productivity(input_2, spark)
df_agronomic = crear_df_agronomic(input_3, spark)
df_sensors = crear_df_sensors(input_4, spark)


for df, table in [
    (df_farms, "farms"),
    (df_productivity, "productivity"),
    (df_agronomic, "agronomic"),
    (df_sensors, "sensors")
]:
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "proyectofinal") \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()