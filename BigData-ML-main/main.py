# Importar bibliotecas

import pyspark
import sys
import os
from pyspark.sql import SparkSession
from functions import crear_df_farms, crear_df_productivity, crear_df_agronomic,crear_df_sensors


# Crear la sesion

spark = SparkSession.builder.appName("Proyecto Final - Transformacion").getOrCreate()


# Inputs para dataframes

base_path = os.path.dirname(os.path.abspath(__file__))

input_1 = sys.argv[1]
input_2 = sys.argv[2]
input_3 = sys.argv[3]
input_4 = sys.argv[4]

# Cargar csv y crear dataframes limpios

df_farms = crear_df_farms(input_1, spark)
df_productivity = crear_df_productivity(input_2, spark)
df_agronomic = crear_df_agronomic(input_3, spark)
df_sensors = crear_df_sensors(input_4, spark)

df_farms.show(6)
df_productivity.show(6)
df_agronomic.show(6)
df_sensors.show(6)
