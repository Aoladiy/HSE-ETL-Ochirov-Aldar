#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

KAFKA_BOOTSTRAP = "rc1b-m3a5ocqmtkp42js3.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "dataproc-kafka-topic"
KAFKA_USER = "user1"
KAFKA_PASSWORD = "password1"

# Схема JSON
schema = StructType([
    StructField("application_id", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType()),
        StructField("term_months", IntegerType())
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType()),
        StructField("risk_level", StringType())
    ])),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("status", StringType())
    ]))),
    StructField("decision_status", StringType()),
    StructField("submitted_at", StringType())
])


def main():
    spark = SparkSession.builder.appName("kafka-read-loans").getOrCreate()

    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username={KAFKA_USER} "
                f"password={KAFKA_PASSWORD} ;") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .where(col("json_str").isNotNull())

    # Парсим JSON
    parsed = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Разворачиваем вложенные поля в плоский вид
    flat = parsed.select(
        col("application_id"),
        col("customer.customer_id").alias("customer_id"),
        col("customer.region").alias("region"),
        col("loan.amount").alias("loan_amount"),
        col("loan.term_months").alias("term_months"),
        col("scoring.score").alias("score"),
        col("scoring.risk_level").alias("risk_level"),
        col("decision_status"),
        col("submitted_at"),
        col("documents")[0]["type"].alias("doc_type"),
        col("documents")[0]["status"].alias("doc_status")
    )

    flat.printSchema()
    print(f"Прочитано записей: {flat.count()}")

    # Сохраняем результат в Object Storage
    flat.write \
        .mode("overwrite") \
        .parquet("s3a://default-bucket-1234/kafka-output/")


if __name__ == "__main__":
    main()
