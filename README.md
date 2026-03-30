# Sales Big Data Project

## Descripción
Proyecto de análisis de ventas usando Apache Spark en modo batch y streaming.

## Tecnologías
- Apache Spark
- Apache Kafka
- Python

## Estructura
- batch/: procesamiento histórico
- streaming/: procesamiento en tiempo real

## Ejecución

### 1. Iniciar servicios
Zookeeper y Kafka

./zookeeper-server-start.sh ../config/zookeeper.properties
 
./kafka-server-start.sh ../config/server.properties

### 2. Crear topic
kafka-topics.sh --create \ --topic sales_topic \ --bootstrap-server localhost:9092 \ --partitions 1 \ --replication-factor 1

### 3. Ejecutar producer
cd streaming
python3 producer.py

### 4. Ejecutar streaming
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_streaming.py

### 5. Ejecutar batch
cd batch
python3 batch_processing.py