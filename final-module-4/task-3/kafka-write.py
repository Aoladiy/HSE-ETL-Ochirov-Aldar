#!/usr/bin/env python3

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

KAFKA_BOOTSTRAP = "rc1b-m3a5ocqmtkp42js3.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "dataproc-kafka-topic"
KAFKA_USER = "user1"
KAFKA_PASSWORD = "password1"


def main():
    spark = SparkSession.builder.appName("kafka-write-loans").getOrCreate()

    # Генерируем ~28 МБ данных — примерно 70000 сообщений по ~400 байт
    messages = []
    for i in range(70000):
        msg = json.dumps({
            "application_id": f"loan_{700000 + i}",
            "customer": {
                "customer_id": f"cust_{i % 1000}",
                "region": "DE-HE"
            },
            "loan": {
                "amount": 10000 + (i % 40000),
                "term_months": 12 + (i % 48)
            },
            "scoring": {
                "score": 600 + (i % 250),
                "risk_level": ["low", "medium", "high"][i % 3]
            },
            "documents": [
                {
                    "type": "passport",
                    "status": "verified"
                }
            ],
            "decision_status": ["approved", "rejected", "manual_review"][i % 3],
            "submitted_at": f"2026-05-{(i % 28) + 1:02d}T10:15:11Z"
        })
        messages.append((msg,))

    df = spark.createDataFrame(messages, ["value"])

    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username={KAFKA_USER} "
                f"password={KAFKA_PASSWORD} ;") \
        .save()

    print(f"Записано сообщений: {len(messages)}")


if __name__ == "__main__":
    main()
