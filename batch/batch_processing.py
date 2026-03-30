from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, expr

# 🔹 Crear sesión Spark
spark = SparkSession.builder \
    .appName("BatchSalesAnalysis") \
    .getOrCreate()

# 🔹 Ruta del CSV (AJUSTA si cambia)
csv_path = "/home/vboxuser/sales-bigdata-project/data/Super_Store_data.csv"

# 🔹 Leer CSV correctamente (evita errores de comillas)
df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True,
    multiLine=True,
    quote='"',
    escape='"'
)

print("=== COLUMNAS DEL DATASET ===")
print(df.columns)

df.show(5)

# 🔹 Limpieza
df = df.dropna()

# 🔹 Conversión de tipos segura
df = df.withColumn("Sales", expr("try_cast(Sales as double)"))
df = df.withColumn("Order Date", to_date(col("Order Date"), "M/d/yyyy"))

# Eliminar registros inválidos
df = df.dropna(subset=["Sales"])

# ===============================
# 📊 ANÁLISIS (EDA)
# ===============================

# 🔹 Ventas por categoría
sales_category = df.groupBy("Category") \
    .agg(sum("Sales").alias("Total_Sales"))

print("\n=== 📊 VENTAS POR CATEGORÍA ===")
sales_category.show()

# 🔹 Ventas por producto
sales_product = df.groupBy("Product Name") \
    .agg(sum("Sales").alias("Total_Sales"))

print("\n=== 📦 TOP 10 PRODUCTOS ===")
sales_product.orderBy(col("Total_Sales").desc()).show(10)

# 🔹 Ventas por fecha
sales_date = df.groupBy("Order Date") \
    .agg(sum("Sales").alias("Total_Sales"))

print("\n=== 📅 VENTAS POR FECHA ===")
sales_date.show()

# 🔹 Total general
print("\n=== 💰 VENTAS TOTALES ===")
df.agg(sum("Sales")).show()

# ===============================
# 🏆 PRODUCTO MÁS VENDIDO
# ===============================

top_product = sales_product.orderBy(col("Total_Sales").desc()).limit(1)

print("\n=== 🏆 PRODUCTO MÁS VENDIDO ===")
top_product.show()

# Mostrar bonito
top = top_product.collect()[0]

print("\n🏆 RESULTADO FINAL:")
print(f"Producto más vendido: {top['Product Name']}")
print(f"Ventas totales: {top['Total_Sales']}")