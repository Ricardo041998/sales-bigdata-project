from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("SalesStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de datos
schema = StructType([
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", FloatType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .load()

# Convertir JSON
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Calcular ventas totales por categoría
result = df_parsed.groupBy("category") \
    .agg(sum(col("price") * col("quantity")).alias("total_sales"))

# Mostrar en consola
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()