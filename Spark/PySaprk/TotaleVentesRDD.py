# Importez les bibliothèques PySpark nécessaires
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import DoubleType

# Créez une configuration Spark et un contexte Spark
spark = SparkConf().setAppName("TP Pyspark Vente RDD")
sc = SparkContext(conf=spark)
# Chargez le fichier CSV dans un RDD Spark en utilisant la fonction textFile() :
rddLines = sc.textFile("ventes.csv")
print("\t \t Le contenu de fichier 'ventes.cvs' \n ")
rddLines.foreach(print)

# Utilisez la fonction filter() pour exclure la première ligne contenant l'en-tête
ventes_lignes = rddLines.filter(lambda ligne: "ville" not in ligne)

print("\t \t Le contenu de fichier 'ventes.cvs' sans lignes \n ")
ventes_lignes.foreach(print)

# Utilisez la fonction map() pour extraire les ventes pour chaque ville
ventes_ville = ventes_lignes.map(lambda ligne: ligne.split(",")) \
    .map(lambda ville_vente: (ville_vente[1], float(ville_vente[3]))) \
    .reduceByKey(lambda x, y: x + y)

# Cette commande crée un RDD Spark qui contient les totaux des ventes pour chaque ville.
ventes_ville.foreach(print)

# Affichez le résultat pour vérifier que les totaux ont été calculés correctement
ventes_ville.saveAsTextFile("ventes_resultat.txt")
# cette commande enregistre le RDD des totaux des ventes dans un fichier texte.
# Fermez le contexte Spark
sc.stop()

## Ces étapes vous permettront de calculer les totaux des ventes dans une ville qui existe dans un fichier CSV avec un en-tête à l'aide de PySpark RDD.

