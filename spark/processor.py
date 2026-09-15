import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Define o formato que esperamos receber do Kafka
earthquake_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth_km", DoubleType(), True),
    StructField("event_time", LongType(), True),
])


# Cria a sessão do Spark
spark = (
    SparkSession.builder
    .appName("EarthquakeStreamProcessor")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Lê continuamente as mensagens do Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "earthquakes")
    .option("startingOffsets", "earliest")
    .load()
)


# Transforma os bytes recebidos do Kafka em texto
json_stream = kafka_stream.select(
    col("value").cast("string").alias("json")
)


# Converte o JSON usando o schema definido acima
parsed_stream = json_stream.select(
    from_json(
        col("json"),
        earthquake_schema
    ).alias("earthquake")
)


# Transforma os campos internos em colunas
earthquakes = parsed_stream.select(
    "earthquake.*"
)


# Função executada para cada novo batch do streaming
def write_to_postgres(batch_df, batch_id):

    # Remove eventos repetidos dentro do próprio batch
    batch_df = batch_df.dropDuplicates(["event_id"])

    # Lê os IDs que já existem no PostgreSQL
    existing_ids = (
        spark.read
        .format("jdbc")
        .option(
                "url",
                f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}")
        .option("dbtable", "earthquakes")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("event_id")
    )

    # Mantém somente terremotos que ainda não existem no banco
    new_earthquakes = batch_df.join(
        existing_ids,
        on="event_id",
        how="left_anti"
    )

    # Guarda o resultado na memória para não recalcular o JOIN
    new_earthquakes = new_earthquakes.cache()

    # Conta quantos terremotos novos encontramos
    new_count = new_earthquakes.count()
    

    # Grava os novos terremotos no PostgreSQL
    (
        new_earthquakes.write
        .format("jdbc")
        .option(
                "url",
                f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}")
        .option("dbtable", "earthquakes")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(
        f"Batch {batch_id}: "
        f"{new_count} novos terremotos gravados."
    )

    new_earthquakes.unpersist()


# Para cada batch recebido, chama nossa função
query = (
    earthquakes.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/earthquakes"
    )
    .start()
)


# Mantém o streaming funcionando
query.awaitTermination()