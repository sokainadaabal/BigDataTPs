# TP Ventes avec Structured Streaming en pyhton

## Introduction 
## C'est quoi PySpark 

## Implémentation
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

#### le contenu de fichier "ventes.csv"
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
### Streaming
## Conclusion

