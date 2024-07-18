from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from clickhouse_driver import Client
import os
import pandas as pd
import json
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.0 --packages org.apache.spark:spark-avro_2.12:3.5.0 pyspark-shel>

with open('/opt/spark/Streams/credentials.json') as json_file:
    connect_settings = json.load(json_file)

ch_db_name = "tmp"
ch_dst_table = "tareLoad_edu"

client = Client(connect_settings['ch_local'][0]['host'],
                user=connect_settings['ch_local'][0]['user'],
                password=connect_settings['ch_local'][0]['password'],
                verify=False,
                database=ch_db_name,
                settings={"numpy_columns": True, 'use_numpy': True},
                compression=True)

spark_app_name = "tareLoad_edu_simple"
spark_ui_port = "8081"

kafka_host = connect_settings['kafka'][0]['host']
kafka_port = connect_settings['kafka'][0]['port']
kafka_topic = "kafka_spark"
kafka_batch_size = 50
processing_time = "5 seconds"
checkpoint_path = f'/opt/kafka_checkpoint_dir/{spark_app_name}/{kafka_topic}/v1'

spark = SparkSession \
    .builder \
    .appName(spark_app_name) \
    .config('spark.ui.port', spark_ui_port) \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.executor.cores", "1") \
    .config("spark.task.cpus", "1") \
    .config("spark.num.executors", "1") \
    .config("spark.executor.instances", "1") \
    .config("spark.default.parallelism", "1") \
    .config("spark.cores.max", "1") \
    .config('spark.ui.port', spark_ui_port) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.debug.maxToStringFields", 500)

dt_schema = StructType([
    StructField("$date", StringType(), True)
])

schema = StructType([
    StructField("tare_id", LongType(), False),
    StructField("dt", dt_schema, True),
    StructField("office_id", LongType(), True),
    StructField("is_pvz", LongType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:{kafka_port}") \
    .option("subscribe", kafka_topic) \
    .option("maxOffsetsPerTrigger", kafka_batch_size) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .load()

# Разбор JSON данных из Kafka
df_parsed = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), ArrayType(schema)).alias("data")) \
    .select(explode(col("data")).alias("data")) \
    .select("data.*")

columns_to_ch = ("tare_id", "dt", "office_id", "is_pvz")

sql_tmp_create = """CREATE TABLE IF NOT EXISTS tmp.tmp_tareLoad_edu
                (
                    tare_id     Int64,
                    dt          DateTime,
                    office_id   Int32,
                    is_pvz      Int8

                ) ENGINE = Memory
                """

sql_insert = f"""INSERT INTO {ch_db_name}.{ch_dst_table}
                select tare_id
                     , dt
                     , office_id
                     , office_name
                     , is_pvz
                from tmp.tmp_tareLoad_edu a
                any left join
                (
                    select office_id
                         , office_name
                    from default.BranchOffice_crop
                ) b
                on a.office_id = b.office_id
              """

def column_filter(df):
    df.show(5)  # For debugging: show DataFrame content
    return df

def load_to_ch(df):
    df_pd = df.toPandas()
    df.select('dt').show()
    df_pd['tare_id'] = df_pd['tare_id'].fillna(0).astype('int64')
    print(df_pd['dt'])
    df_pd['dt'] = df_pd['dt'].apply(lambda x: x[0])
    df_pd['dt'] = df_pd['dt'].str.strip('{},()')
    print(df_pd['dt'])
    df_pd.dt = pd.to_datetime(df_pd.dt, format='ISO8601', utc=True, errors='ignore')
    print(df_pd['dt'])
    df_pd['office_id'] = df_pd['office_id'].fillna(0).astype('int32')
    df_pd['is_pvz'] = df_pd['is_pvz'].fillna(0).astype('int8')

    client.insert_dataframe('INSERT INTO tmp.tmp_tareLoad_edu VALUES', df_pd)

def foreach_batch_function(df2, epoch_id):
    df_rows = df2.count()
    if df_rows > 0:
        df2.show(5)
        df2 = column_filter(df2)
        client.execute(sql_tmp_create)
        load_to_ch(df2)
        client.execute(sql_insert)
        client.execute("DROP TABLE IF EXISTS tmp.tmp_tareLoad_edu")

query = df_parsed.writeStream \
        .trigger(processingTime=processing_time) \
        .option("checkpointLocation", checkpoint_path) \
        .foreachBatch(foreach_batch_function) \
        .start()

query.awaitTermination()
