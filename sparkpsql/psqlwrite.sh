#!/bin/bash


spark-submit \
  --driver-class-path postgresql-42.2.14.jar \
  --jars postgresql-42.2.14.jar \
  psqlwrite.py ../farms.csv ../productivity.csv ../agronomic_data.csv ../sensors_data.csv


