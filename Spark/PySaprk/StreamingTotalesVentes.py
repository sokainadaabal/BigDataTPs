from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType


schema = StructType([
    StructField("date", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("prix", FloatType(), True)
])
spark = SparkSession.builder.appName("TotalVentesParVille").getOrCreate()

ventes = spark.readStream.format("csv").option("header", "true").schema(schema).load("ventes")

total_ventes_par_ville = ventes.groupBy("ville").agg(sum("prix"))

query = total_ventes_par_ville.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

