# 🧊 Spring Boot Iceberg Spark Example

Example project showing how to use **Apache Iceberg** with **Java** — both through the **native Iceberg Java API** and via **Apache Spark** integration.  
Built with **Spring Boot** to demonstrate practical use cases of data ingestion and querying on Iceberg tables.

---

## 🚀 Overview

This project provides two main examples of interacting with Iceberg tables:

1. **Native Iceberg API (Java)**
    - Demonstrates direct use of the Apache Iceberg Java library for reading and writing data.
    - Useful when you want fine-grained control without relying on a SQL engine.

2. **Apache Spark + Iceberg**
    - Demonstrates how to use Spark to perform data operations (inserts, queries) on Iceberg tables.
    - Showcases integration via SparkSession and the Iceberg Spark extensions.

---

## 🧱 Tech Stack

| Component | Description |
|------------|--------------|
| **Java 17+** | Main programming language |
| **Spring Boot** | Application framework |
| **Apache Iceberg** | Table format for large analytic datasets |
| **Apache Spark** | Distributed processing engine |
| **Parquet** | Columnar data file format used by Iceberg |
| **Maven** | Build tool |

---

## 🗂️ Project Structure

