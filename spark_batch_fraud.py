#El script spark_batch_fraud.py se encargó de:

#1. Cargar el dataset desde el archivo creditcard.csv.

#2. Inferir el esquema de los datos y eliminar registros nulos.

#3. Calcular estadísticas básicas, como número de transacciones, cantidad de fraudes detectados y montos promedio.

#4. Guardar los resultados procesados en un archivo CSV limpio y estructurado.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, count

spark = SparkSession.builder.appName("FraudBatchProcessing").getOrCreate()

df = spark.read.csv("/home/vboxuser/creditcard.csv", header=True, inferSchema=True)
df_clean = df.dropna()

# Análisis exploratorio
fraud_stats = df_clean.groupBy("Class").agg(
    count("*").alias("total_transacciones"),
    mean("Amount").alias("monto_promedio")
)

fraud_stats.show()
df_clean.write.csv("/home/vboxuser/output/creditcard_clean.csv", header=True)
spark.stop()
