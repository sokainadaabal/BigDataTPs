from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType


schema = StructType([
    StructField("date", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("prix", FloatType(), True)
])
# Créer une session Spark
spark = SparkSession.builder.appName("TotalVentesParVille").getOrCreate()

# Lire le fichier CSV en tant que source de streaming
ventes = spark.readStream.format("csv").option("header", "true").schema(schema).load("ventes")

# Sélectionner les colonnes "ville" et "prix_vente", puis grouper par ville et calculer la somme des prix de vente
total_ventes_par_ville = ventes.groupBy("ville").agg(sum("prix"))

# Définir la sortie en tant que console pour voir les résultats à mesure qu'ils sont calculés
query = total_ventes_par_ville.writeStream.outputMode("complete").format("console").start()

# Attendre que le traitement se termine
query.awaitTermination()

