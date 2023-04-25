from pyspark import SparkContext, SparkConf

spark = SparkConf().setAppName("TP Pyspark Vente RDD")
sc = SparkContext(conf=spark)

rddLines = sc.textFile("ventes.csv")
print("\t \t Le contenu de fichier 'ventes.cvs' \n ")
rddLines.foreach(print)

ventes_lignes = rddLines.filter(lambda ligne: "ville" not in ligne)
print("\t \t Le contenu de fichier 'ventes.cvs' sans lignes \n ")
ventes_lignes.foreach(print)

ventes_ville = ventes_lignes.map(lambda ligne: ligne.split(",")) \
    .map(lambda ville_vente: (ville_vente[1], float(ville_vente[3]))) \
    .reduceByKey(lambda x, y: x + y)

ventes_ville.foreach(print)

ventes_ville.saveAsTextFile("ventes_resultat.txt")

sc.stop()

