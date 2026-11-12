import os
from datetime import datetime, timezone

import pendulum
import requests
from pymongo import MongoClient, UpdateOne
from airflow.sdk import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
SOURCE = "Frankfurter API"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rates -> MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task
    def extract_exchange_rates(**context):
        ti = context["ti"]
        base = os.getenv("BASE_CURRENCY", "EUR")
        quotes = os.getenv("TARGET_CURRENCIES", "USD,GBP,JPY")

        response = requests.get(
            API_URL, params={"base": base, "quotes": quotes}, timeout=30
        )
        response.raise_for_status()
        raw_data = response.json()

        ti.xcom_push(key="raw_exchange_rates", value=raw_data)
        ti.xcom_push(
            key="retrieved_at",
            value=datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat(),
        )
        print(f"Extracted {len(raw_data)} rates: {raw_data}")

    @task
    def transform_exchange_rates(**context):
        ti = context["ti"]
        raw_data = ti.xcom_pull(
            task_ids="extract_exchange_rates", key="raw_exchange_rates"
        )
        retrieved_at = ti.xcom_pull(
            task_ids="extract_exchange_rates", key="retrieved_at"
        )

        transformed = [
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": row["rate"],
                "rate_date": row["date"],
                "source": SOURCE,
                "retrieved_at": retrieved_at,
            }
            for row in raw_data
        ]

        ti.xcom_push(key="transformed_exchange_rates", value=transformed)
        print(f"Transformed records: {transformed}")

    @task
    def load_to_mongodb(**context):
        ti = context["ti"]
        records = ti.xcom_pull(
            task_ids="transform_exchange_rates", key="transformed_exchange_rates"
        )

        client = MongoClient(os.getenv("MONGO_URI"))
        collection = client[os.getenv("MONGO_DB", "currency_db")][
            os.getenv("MONGO_COLLECTION", "exchange_rates")
        ]

        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)
        print(f"Upserted: {result.upserted_count}, modified: {result.modified_count}")
        client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()