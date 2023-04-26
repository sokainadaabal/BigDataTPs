# TP Ventes avec Structured Streaming en pyhton

## Introduction 
Le but de ce TP est d'utiliser Apache Spark avec PySpark pour calculer le total des ventes par ville. Pour cela, nous allons utiliser dans un premeir instant les RDD puis les dataframes  et vers la fin Structured Streaming qu' une API de traitement en continu des données dans Spark, c'est la partie important dans le tp. Structured Streaming fournit une interface simplifiée pour le traitement en continu de données et permet de facilement effectuer des agrégations sur les flux de données.
## C'est quoi PySpark 
PySpark est un framework open source pour le traitement des données à grande échelle basé sur Apache Spark et conçu pour être utilisé avec Python. Il permet d'effectuer des opérations de traitement de données distribuées sur des clusters de serveurs en utilisant des méthodes efficaces de calcul en mémoire.

PySpark utilise un modèle de programmation parallèle pour diviser les données et les calculs en blocs qui peuvent être traités simultanément sur plusieurs nœuds de calcul. Cela permet d'accélérer le traitement des données et de gérer des ensembles de données massives en utilisant des techniques telles que le partitionnement et la distribution.

PySpark est capable de traiter différents types de données tels que les RDD (Resilient Distributed Datasets), les DataFrames et les datasets structurés. Il est également doté d'une bibliothèque complète de fonctions de traitement de données telles que les fonctions de filtrage, de tri, d'agrégation, de jointure et de transformation.

En utilisant PySpark, les utilisateurs peuvent facilement traiter des données massives, effectuer des analyses avancées et créer des modèles de machine learning en utilisant les bibliothèques intégrées telles que Spark SQL, Spark Streaming et MLlib.
## Implémentation
#### le contenu de fichier "ventes.csv" sur le quel on va faire le traitement
```
date,ville,produit,prix
2020/12/01,Casablanca,HP,85400.31199999999
2023/12/01,Guercif Oulad,HP,94200.35299999999
2022/12/01,Qasbat Tadla,LENOVO,118166.463
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2019/12/01,Larach,Mac,94059.21200000001
2020/12/01,Guelmim,LENOVO,75337.70999999999
2020/12/01,Inezgane,HEWAWI,66242.342
2019/12/01,Kouribga,HP,53104.721000000005
2020/12/01,Marrakech,Dell,109202.587
2020/12/01,Kenitra,Bic,129840.834
2020/12/01,Essaouira,HP,124884.69899999998
2020/12/01,Sidi Slimane,HP,199364.18500000003
2020/12/01,Sidi Slimane,HP,199364.1500000006
2020/12/01,Kenitra,Bic,12987840.834
```
### RDD
> Importez les bibliothèques PySpark nécessaires
```python
from pyspark import SparkContext, SparkConf
```
> Créez une configuration Spark et un contexte Spark
```python
spark = SparkConf().setAppName("TP Pyspark Vente RDD")
sc = SparkContext(conf=spark)
```
> Chargez le fichier CSV dans un RDD Spark en utilisant la fonction textFile() 
```python
rddLines = sc.textFile("ventes.csv")
print("\t \t Le contenu de fichier 'ventes.cvs' \n ")
rddLines.foreach(print)
```
> Utilisez la fonction filter() pour exclure la première ligne contenant l'en-tête
```python
ventes_lignes = rddLines.filter(lambda ligne: "ville" not in ligne)
print("\t \t Le contenu de fichier 'ventes.cvs' sans lignes \n ")
ventes_lignes.foreach(print)
```
> Utilisez la fonction map() pour extraire les ventes pour chaque ville
```python
ventes_ville = ventes_lignes.map(lambda ligne: ligne.split(",")) \
    .map(lambda ville_vente: (ville_vente[1], float(ville_vente[3]))) \
    .reduceByKey(lambda x, y: x + y)
```
> Cette commande crée un RDD Spark qui contient les totaux des ventes pour chaque ville.
```python
ventes_ville.foreach(print)
```
> Affichez le résultat pour vérifier que les totaux ont été calculés correctement
```python
ventes_ville.saveAsTextFile("ventes_resultat.txt")
```
> cette commande enregistre le RDD des totaux des ventes dans un fichier texte et Fermez le contexte Spark.
```python
sc.stop()
```
> Ces étapes vous permettront de calculer les totaux des ventes dans une ville qui existe dans un fichier CSV avec un en-tête à l'aide de PySpark RDD.


#### test
```
Le contenu de fichier 'ventes.cvs' 
 
2020/12/01,Inezgane,HEWAWI,66242.342
2019/12/01,Kouribga,HP,53104.721000000005
2020/12/01,Marrakech,Dell,109202.587
2020/12/01,Kenitra,Bic,129840.834
2020/12/01,Essaouira,HP,124884.69899999998
2020/12/01,Sidi Slimane,HP,199364.18500000003
2020/12/01,Sidi Slimane,HP,199364.1500000006
2020/12/01,Kenitra,Bic,12987840.834
date,ville,produit,prix
2020/12/01,Casablanca,HP,85400.31199999999
2023/12/01,Guercif Oulad,HP,94200.35299999999
2022/12/01,Qasbat Tadla,LENOVO,118166.463
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2019/12/01,Larach,Mac,94059.21200000001
2020/12/01,Guelmim,LENOVO,75337.70999999999
	 	 Le contenu de fichier 'ventes.cvs' sans lignes 
 
2020/12/01,Casablanca,HP,85400.31199999999
2023/12/01,Guercif Oulad,HP,94200.35299999999
2022/12/01,Qasbat Tadla,LENOVO,118166.463
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2020/12/01,El Kelaa des Srarhna,HP,181711.51599999997
2019/12/01,Larach,Mac,94059.21200000001
2020/12/01,Guelmim,LENOVO,75337.70999999999
2020/12/01,Inezgane,HEWAWI,66242.342
2019/12/01,Kouribga,HP,53104.721000000005
2020/12/01,Marrakech,Dell,109202.587
2020/12/01,Kenitra,Bic,129840.834
2020/12/01,Essaouira,HP,124884.69899999998
2020/12/01,Sidi Slimane,HP,199364.18500000003
2020/12/01,Sidi Slimane,HP,199364.1500000006
2020/12/01,Kenitra,Bic,12987840.834
('Qasbat Tadla', 118166.463)
('El Kelaa des Srarhna', 363423.03199999995)
('Guelmim', 75337.70999999999)
('Inezgane', 66242.342)
('Kenitra', 13117681.668000001)
('Casablanca', 85400.31199999999)
('Guercif Oulad', 94200.35299999999)
('Larach', 94059.21200000001)
('Kouribga', 53104.721000000005)
('Marrakech', 109202.587)
('Essaouira', 124884.69899999998)
('Sidi Slimane', 398728.33500000066)
```
### DF
> Importez les bibliothèques PySpark nécessaires
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
```
> Créez une session Spark
```python
spark = SparkSession.builder.appName("TP Pyspark Vente").master("local[*]").getOrCreate()
```
> Chargez le fichier CSV dans un DataFrame Spark
```python
dfLines = spark.read.format("csv").option('delimiter', ',').option("schema", "customerSchema").option("inferSchema", "true").option("header", "true").load("ventes.csv")
dfLines.printSchema()
print("\t \t Le contenu de fichier 'ventes.cvs' \n ")
dfLines.show()  # afficher le contenu de fichier sous forme de tableau
```
> Utilisez la fonction groupBy() pour regrouper les données par ville et la fonction sum() pour calculer la somme des ventes par ville 
> Cette commande crée un nouveau DataFrame qui contient les totaux des ventes pour chaque ville.
```python
df_villes = dfLines.groupBy("ville").agg(sum("prix").alias("totales_prix"))
```
> Affichez le résultat pour vérifier que les totaux ont été calculés correctement
```python
df_villes.show()
```
> Enregistrez le résultat dans un fichier CSV
```python
df_villes.write.format("csv").option("header", "true").save("totale_Ventes.csv")
```
> Fermer la sessions de spark
```python
spark.stop()
```
> Ces étapes vous permettront de calculer les totaux des ventes dans une ville qui existe dans un fichier CSV à l'aide de PySpark.

#### test 
```
root
 |-- date: string (nullable = true)
 |-- ville: string (nullable = true)
 |-- produit: string (nullable = true)
 |-- prix: double (nullable = true)

	 	 Le contenu de fichier 'ventes.cvs' 
 
+----------+--------------------+-------+------------------+
|      date|               ville|produit|              prix|
+----------+--------------------+-------+------------------+
|2020/12/01|          Casablanca|     HP| 85400.31199999999|
|2023/12/01|       Guercif Oulad|     HP| 94200.35299999999|
|2022/12/01|        Qasbat Tadla| LENOVO|        118166.463|
|2020/12/01|El Kelaa des Srarhna|     HP|181711.51599999997|
|2020/12/01|El Kelaa des Srarhna|     HP|181711.51599999997|
|2019/12/01|              Larach|    Mac| 94059.21200000001|
|2020/12/01|             Guelmim| LENOVO| 75337.70999999999|
|2020/12/01|            Inezgane| HEWAWI|         66242.342|
|2019/12/01|            Kouribga|     HP|53104.721000000005|
|2020/12/01|           Marrakech|   Dell|        109202.587|
|2020/12/01|             Kenitra|    Bic|        129840.834|
|2020/12/01|           Essaouira|     HP|124884.69899999998|
|2020/12/01|        Sidi Slimane|     HP|199364.18500000003|
|2020/12/01|        Sidi Slimane|     HP| 199364.1500000006|
|2020/12/01|             Kenitra|    Bic|    1.2987840834E7|
+----------+--------------------+-------+------------------+

+--------------------+--------------------+
|               ville|        totales_prix|
+--------------------+--------------------+
|        Qasbat Tadla|          118166.463|
|          Casablanca|   85400.31199999999|
|            Kouribga|  53104.721000000005|
|           Essaouira|  124884.69899999998|
|       Guercif Oulad|   94200.35299999999|
|             Kenitra|1.3117681668000001E7|
|            Inezgane|           66242.342|
|El Kelaa des Srarhna|  363423.03199999995|
|        Sidi Slimane|  398728.33500000066|
|              Larach|   94059.21200000001|
|             Guelmim|   75337.70999999999|
|           Marrakech|          109202.587|
+--------------------+--------------------+

```

### Streaming
> Importez les bibliothèques PySpark nécessaires
```python
from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
```
> Créez une schéma
```python
schema = StructType([
    StructField("date", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("prix", FloatType(), True)
])
```
> Créez une session Spark
```python
spark = SparkSession.builder.appName("TotalVentesParVille").getOrCreate()
```
> Lire le fichier CSV en tant que source de streaming
```python
ventes = spark.readStream.format("csv").option("header", "true").schema(schema).load("ventes")
```
> Sélectionner les colonnes "ville" et "prix_vente", puis grouper par ville et calculer la somme des prix de vente
```python
total_ventes_par_ville = ventes.groupBy("ville").agg(sum("prix"))
```
> Définir la sortie en tant que console pour voir les résultats à mesure qu'ils sont calculés
```python

query = total_ventes_par_ville.writeStream.outputMode("complete").format("console").start()
```
> Attendre que le traitement se termine
```python
query.awaitTermination()
```
#### test 
1. lorsque lance le programme va lire le fichier et affiche c'est resulat puis attend s'il y a d'autre données saisie pour refaire le traitement
```
-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+--------------+
|        ville|     sum(prix)|
+-------------+--------------+
| Qasbat Tadla|118166.4609375|
|   Casablanca|    85400.3125|
|Guercif Oulad| 94200.3515625|
|         moha|118166.4609375|
+-------------+--------------+

```
2. saisir quelque nouvelle instruction
```
2022/12/01,Casa,LENOVO,118166.463
2022/12/01,Paris,LENOVO,118166.463
```
3. la résultat s'affiche
``` 
-------------------------------------------
Batch: 1
-------------------------------------------
+-------------+--------------+
|        ville|     sum(prix)|
+-------------+--------------+
| Qasbat Tadla| 236332.921875|
|   Casablanca|    170800.625|
|Guercif Oulad| 188400.703125|
|        Paris|118166.4609375|
|         Casa|118166.4609375|
|         moha| 236332.921875|
+-------------+--------------+
```
## Conclusion
Ce TP nous a permis de comprendre les différences entre les RDD, les DataFrames et le Streaming dans PySpark, ainsi nous avons pu constater la simplicité et la puissance de l'API Structured Streaming pour le traitement en continu des données dans Spark. Nous avons vu que PySpark offre également une grande flexibilité pour traiter des données à grande échelle en utilisant des clusters de calculs distribués. En somme, ce TP nous a permis de découvrir les bases de PySpark et de Structured Streaming et de comprendre comment utiliser ces outils pour traiter des données de grande taille en continu.
