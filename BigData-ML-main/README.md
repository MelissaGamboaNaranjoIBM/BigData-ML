# Big Data Processing & Machine Learning Pipeline with Apache Spark

## 📌 Project Overview
This project implements a **Big Data processing pipeline** using **Apache Spark**, integrating data ingestion, transformation, storage, and basic machine learning workflows.

The solution demonstrates how to process large datasets in a distributed environment, persist results into a relational database, and validate logic through automated unit testing.

---

## 🎯 Project Objectives
- Process large-scale datasets using Apache Spark.
- Apply data transformations and feature preparation in a distributed environment.
- Persist processed data into a PostgreSQL database.
- Execute the pipeline using containerized services.
- Validate data processing logic using unit tests.

---

## 🏗️ Architecture Overview
The project follows a modular Big Data architecture:

1. **Data Processing Layer**
   - Apache Spark used for distributed data transformations.
   - Business logic separated into reusable functions.

2. **Persistence Layer**
   - PostgreSQL used as the target database.
   - Spark writes processed data into relational tables.

3. **Orchestration & Execution**
   - Shell scripts to manage container execution and database connections.
   - Spark job executed through a main driver script.

4. **Testing Layer**
   - Unit tests implemented using `pytest` to validate core logic.

---

## 🧠 Machine Learning Component
A basic machine learning workflow is included to demonstrate:
- Feature preparation using Spark.
- Model training and evaluation.
- Integration of ML logic within a Big Data pipeline.

This component highlights how analytical and ML tasks can coexist within Spark-based systems.


