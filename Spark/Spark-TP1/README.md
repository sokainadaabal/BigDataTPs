<table>
  <tr >
    <th style="text-align: center;">Ecole Normale Supérieure de l’Enseignement  Technique Mohammedia Université Hassan II de Casablanca</th>
    <th><img src="https://www.clubs-etudiants.ma/wp-content/uploads/2018/11/enst-1.png"/></th>
    <th style="text-align: center;"> Département Mathématiques et Informatique « Ingénierie Informatique : Big Data e t Cloud Computing » II-BDCC2   </th>
  </tr>
</table>
<table >
   <tr >
      <th style="text-align: center;">Réaliser par </th>
      <th style="text-align: center;">  Module  </th>
      <th style="text-align: center;">Filière</th>
      <th style="text-align: center;">Date </th>
      <th style="text-align: center;">  E-mail  </th>
    </tr>
    <tr>
      <td>Daabal Sokaina</td>
      <td> Big data</td>
      <td> II-BDCC 2 </td>
      <td> 10 Mars 2023 </td>
      <td> sokainadaabal@gmail.com / s.daabal@etu.enset-media.ac.ma </td>
     </tr>
</table> 

# TP 1 : Spark
## Introduction 
> Dans ce TP, nous avons la possibilité de voir de quelle façon un fichier .txt peut être traité. Et retourner un résultat avec spark.

## Objectifs

  Utilisation de Spark pour réaliser des traitements sur des données, des fichiers de type txt ou cvs.
## Spark 
### Présentation
Spark est un système de traitement rapide et parallèle. Il fournit des APIs de haut niveau en Java, Scala, Python et R, et un moteur optimisé qui supporte l'exécution des graphes. 
Il supporte également un ensemble d'outils de haut niveau tels que Spark SQL pour le support du traitement de données structurées, MLlib pour l'apprentissage des données, GraphX pour le traitement des graphes, et Spark Streaming pour le traitment des données en streaming

### Spark & Hadoop
Spark peut s'exécuter sur plusieurs plateformes: Hadoop, Mesos, en standalone ou sur le cloud. Il peut également accéder diverses sources de données, comme HDFS, Cassandra, HBase et S3.

### Installation 
Pour installer spark,nous avons des Conditions préalables  tels que :
 1. Un système Ubuntu.
 2. Accès à un terminal ou à une ligne de commande.
 3. Un utilisateur avec des autorisations sudo ou root.
#### Installation des packages requis pour spark :
Avant de passer a l'installation de spark, vous devez installer les dépendances nécessaires. Cette étape inclut l'installation des packages suivants: 

```
 - JDK
 - Scala
 - Gite
```

Ouvrez une fenêtre de terminal et exécutez la commande suivante pour installer les trois packages en même temps:

```
    sudo apt install default-jdk scala git -y
```
Une fois le processus terminé, vérifiez `les dépendances installées` en exécutant ces commandes :

```
java -version; javac -version; scala -version; git --version
```

### Télécharger et configurer Spark sur Ubuntu

Utilisez la wget commande et le lien direct pour télécharger l'archive Spark :
```
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
```
Extrayez l'archive enregistrée en utilisant tar :
```
tar xvf spark-*
```

Déplacez le répertoire décompressé spark-3.0.1-bin-hadoop2.7 vers le répertoire opt/spark .

Utilisez la mv commande pour le faire :

```
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
```

### Configurer l'environnement Spark

Utilisez la echo commande pour ajouter ces trois lignes à .profile :

```
echo "export SPARK_HOME=/opt/spark" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
```
Chargez le fichier .profile dans la ligne de commande en tapant :

```
source ~/.profile
```

### Démarrer le serveur maître Spark autonome

Dans le terminal, tapez :

```
start-master.sh
```
Pour afficher l'interface utilisateur Spark Web, ouvrez un navigateur Web et entrez l' adresse IP localhost sur le port 8080.

```
http://127.0.0.1:8080/
```
 
## Application de Spark
### Appliucation 1
#### C'est quoi RDD
Le RDD est une collection d’éléments partitionnées et distribuées dans le cluster.
