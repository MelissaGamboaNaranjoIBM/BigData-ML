from pyspark.sql.functions import col, avg, when,month
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DecimalType, DoubleType, DateType

def crear_df_farms(input_1, spark):
    farms_schema = StructType([
        StructField("farm_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("crop_type", StringType(), True),
        StructField("irrigation_type", StringType(), True),
        StructField("fertilizer_type", StringType(), True),
        StructField("pesticide_usage_ml", DoubleType(), True)
    ])

    farms = spark.read.csv(input_1, schema=farms_schema, header=True)
    farms = farms.filter(col("farm_id").isNotNull())
    df_farms = farms.dropDuplicates()
    return df_farms

def crear_df_productivity(input_2, spark):
    productivity_schema = StructType([
        StructField("farm_id", StringType(), True),
        StructField("sowing_date", DateType(), True),
        StructField("harvest_date", DateType(), True),
        StructField("yield_kg_per_hectare", DoubleType(), True)
    ])
    productivity = spark.read.csv(input_2, schema=productivity_schema, header=True)
    productivity = productivity.filter(col("farm_id").isNotNull())
    df_productivity = productivity.dropDuplicates()
    df_productivity = df_productivity.withColumn("sowing_month", month(col("sowing_date")))
    return df_productivity


def crear_df_agronomic(input_3, spark):
  agronomic_schema = StructType([
        StructField("farm_id", StringType(), True),
        StructField("soil_moisture_%", DoubleType(), True),
        StructField("soil_pH", DoubleType(), True),
        StructField("temperature_C", DoubleType(), True),
        StructField("rainfall_mm", DoubleType(), True),
        StructField("humidity_%", DoubleType(), True),
        StructField("sunlight_hours", DoubleType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", DateType(), True)
        ])
  agronomic = spark.read.csv(input_3, schema=agronomic_schema, header=True)
  agronomic = agronomic.filter(col("farm_id").isNotNull()).filter(col("sensor_id").isNotNull())
  df_agronomic = agronomic.dropDuplicates()
  return df_agronomic


def crear_df_sensors(input_4, spark):
    sensors_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", DateType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("NDVI_index", DoubleType(), True),
        StructField("crop_disease_status", StringType(), True)
    ])

    sensors = spark.read.csv(input_4, schema=sensors_schema, header=True)

    sensors = sensors.filter(col("sensor_id").isNotNull())
    df_sensors = sensors.dropDuplicates()
    df_sensors = df_sensors.withColumn("timestamp_month", month(col("timestamp")))

    return df_sensors