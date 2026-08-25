import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
from datetime import date
from functions import crear_df_farms, crear_df_productivity, crear_df_agronomic,crear_df_sensors

# Crear la sesion

spark = SparkSession.builder.appName("Proyecto Final").getOrCreate()

# Preparar los inputs manuales

input_1_data = """farm_id,region,crop_type,irrigation_type,fertilizer_type,pesticide_usage_ml
FARM0002,South USA,Soybean,Sprinkler,Inorganic,9.6
,East Africa,Rice,Manual,Organic,31.29
FARM0477,South USA,Soybean,Drip,Inorganic,5.63
FARM0477,South USA,Soybean,Drip,Inorganic,5.63
"""
input_1_manual = "/tmp/farms.csv"
Path(input_1_manual).write_text(input_1_data)


input_2_data = """farm_id,sowing_date,harvest_date,yield_kg_per_hectare
FARM0485,2024-02-02,2024-06-03,3708.29
FARM0485,2024-02-02,2024-06-03,3708.29
,2024-01-05,2024-04-29,3684.22
"""
input_2_manual = "/tmp/productivity.csv"
Path(input_2_manual).write_text(input_2_data)

input_3_data = """farm_id,soil_moisture_%,soil_pH,temperature_C,rainfall_mm,humidity_%,sunlight_hours,sensor_id,timestamp
FARM0016,15.52,7.17,29.07,202.92,89.36,7.92,SENS0016,2024-03-10
,37.89,6.1,28.46,187.19,83.74,05.01,SENS0457,2024-06-12
FARM0019,40.94,6.31,27.41,88.64,86.49,9.19,,2024-05-13
FARM0019,40.94,6.31,27.41,88.64,86.49,9.19,SENS0019,2024-05-13
FARM0016,15.52,7.17,29.07,202.92,89.36,7.92,SENS0016,2024-03-10
"""
input_3_manual = "/tmp/agronomic_data.csv"
Path(input_3_manual).write_text(input_3_data)

input_4_data = """sensor_id,timestamp,latitude,longitude,NDVI_index,crop_disease_status
SENS0119,2024-05-14,16.433.169,86.431.839,0.88,Severe
,2024-02-10,11.829.887,83.889.714,0.75,Severe
SENS0119,2024-05-14,16.433.169,86.431.839,0.88,Severe
"""
input_4_manual = "/tmp/sensors_data.csv"
Path(input_4_manual).write_text(input_4_data)


def test_crear_df_farms_pass():
    # Aplicar la funcion
    df_farms = crear_df_farms(input_1_manual,spark)

    # Este debe ser el resultado
    df_farms_correct = spark.createDataFrame([("FARM0002","South USA","Soybean","Sprinkler","Inorganic",9.6),
                                        ("FARM0477","South USA","Soybean","Drip","Inorganic",5.63)]
                                        , ["farm_id","region","crop_type","irrigation_type","fertilizer_type","pesticide_usage_ml"])

    # Ordenar para evitar errores en el assert, aunque no sea necesario en produccion
    df_farms_sorted = df_farms.orderBy("farm_id")
    df_farms_correct_sorted = df_farms_correct.orderBy("farm_id")

    df_farms_sorted.show()
    df_farms_correct_sorted.show()

    assert df_farms_sorted.collect() == df_farms_correct_sorted.collect()


def test_crear_df_productivity_pass():

    df_productivity = crear_df_productivity(input_2_manual,spark)

    # Este debe ser el resultado
    df_productivity_correct = spark.createDataFrame([("FARM0485",date(2024, 2, 2), date(2024, 6, 3),3708.29,2),]
                                        , ["farm_id","sowing_date","harvest_date","yield_kg_per_hectare","sowing_month"])

    df_productivity.show()
    df_productivity_correct.show()


    assert df_productivity.collect() == df_productivity_correct.collect()


def test_crear_df_agronomic_pass():
  df_agronomic = crear_df_agronomic(input_3_manual,spark)

  # Este debe ser el resultado
  df_agronomic_correct = spark.createDataFrame([("FARM0016",15.52,7.17,29.07,202.92,89.36,7.92,"SENS0016",date(2024, 3, 10)),
                                                  ("FARM0019",40.94,6.31,27.41,88.64,86.49,9.19,"SENS0019",date(2024, 5, 13))]
                                                , ["farm_id","soil_moisture_%","soil_pH","temperature_C","rainfall_mm","humidity_%","sunlight_hours","sensor_id","timestamp"])

  # Ordernar para evitar errores en el assert, aunque no sea necesario en produccion
  df_agronomic_sorted = df_agronomic.orderBy("farm_id")
  df_agronomic_correct_sorted = df_agronomic_correct.orderBy("farm_id")

  df_agronomic_sorted.show()
  df_agronomic_correct_sorted.show()

  assert df_agronomic_sorted.collect() == df_agronomic_correct_sorted.collect()



def test_crear_df_sensors_pass():
    df_sensors = crear_df_sensors(input_4_manual,spark)

    # Este debe ser el resultado
    df_sensors_correct = spark.createDataFrame([("SENS0119",date(2024, 5, 14),"16.433.169","86.431.839",0.88,"Severe",5)]
                                                , ["sensor_id","timestamp","latitude","longitude","NDVI_index","crop_disease_status","timestamp_month"])

    df_sensors.show()
    df_sensors_correct.show()
    assert df_sensors.collect() == df_sensors_correct.collect()


def test_rango_ndvi():
    input_4 = "/tmp/sensors_data.csv"
    spark = SparkSession.builder.getOrCreate()
    df_sensors = crear_df_sensors(input_4, spark)
    df_invalid = df_sensors.filter((df_sensors["NDVI_index"] < -1) | (df_sensors["NDVI_index"] > 1))
    assert df_invalid.count() == 0, "NDVI_index fuera del rango permitido [-1, 1]"

def test_valores_disease_status():
    input_4 = "/tmp/sensors_data.csv"
    spark = SparkSession.builder.getOrCreate()
    df_sensors = crear_df_sensors(input_4, spark)
    valores_esperados = {"Mild", "Moderate", "None","Severe"}
    valores_actuales = set(row["crop_disease_status"] for row in df_sensors.select("crop_disease_status").distinct().collect())
    assert valores_actuales.issubset(valores_esperados), f"Valores inesperados en 'crop_disease_status': {valores_actuales - valores_esperados}"