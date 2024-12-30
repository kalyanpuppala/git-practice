from pyspark.sql import SparkSession
spark=SparkSession.builder\
.appName("Kafkastreaming")\
.master("local[*]")\
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
.getOrCreate()
kafka_brokers="localhost:9092"
kafka_topic="firsttopic"
df=spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers",kafka_brokers)\
    .option("subscribe",kafka_topic)\
    .load()
df.printSchema()
kafka_data = df.selectExpr("CAST(value AS STRING)")

# Print the data to the console
query = kafka_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
