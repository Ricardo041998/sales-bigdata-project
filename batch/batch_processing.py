from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
import kagglehub
import os

# 🔹 1. Descargar dataset
path = kagglehub.dataset_download("zahraaalaatageldein/sales-for-furniture-store")
print("Dataset descargado en:", path)

# 🔹 2. Buscar archivo CSV automáticamente
files = os.listdir(path)
csv_file = [f for f in files if f.endswith(".csv")][0]
csv_path = os.path.join(path, csv_file)

print("Archivo CSV encontrado:", csv_path)

# 🔹 3. Crear sesión Spark
spark = SparkSession.builder \
    .appName("BatchSalesAnalysis") \
    .getOrCreate()

# 🔹 4. Cargar datos
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print("=== Columnas del dataset ===")
print(df.columns)

df.show(5)

# 🔹 5. Limpieza
df = df.dropna()

# 🔹 6. AJUSTE AUTOMÁTICO DE COLUMNAS (IMPORTANTE)
# Vamos a detectar nombres comunes

# Posibles nombres según dataset
date_col = [c for c in df.columns if "date" in c.lower()][0]
sales_col = [c for c in df.columns if "sales" in c.lower() or "revenue" in c.lower()][0]
product_col = [c for c in df.columns if "product" in c.lower()][0]

print(f"Usando columnas -> Fecha: {date_col}, Ventas: {sales_col}, Producto: {product_col}")

# 🔹 7. Transformaciones
df = df.withColumn("sales", col(sales_col).cast("double"))
df = df.withColumn("date", to_date(col(date_col)))

# 🔹 8. EDA

# Ventas por producto
sales_product = df.groupBy(product_col).agg(sum("sales").alias("total_sales"))

print("=== Ventas por producto ===")
sales_product.show()

# Ventas por fecha
sales_date = df.groupBy("date").agg(sum("sales").alias("total_sales"))

print("=== Ventas por fecha ===")
sales_date.show()

# Top productos
print("=== Top productos ===")
sales_product.orderBy(col("total_sales").desc()).show(5)

# Total general
print("=== Total general ===")
df.agg(sum("sales")).show()