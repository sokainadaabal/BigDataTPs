# Sqoop : Connexion a une base de donnee Mysql a distante

## Installation et configurantion de Sqoop 
### Telecharger le package

``` cmd
wget http://apache.mirror.digitalpacific.com.au/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
```

### Decompresser le package

``` cmd 
tar -xvzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz -C ~/hadoop
```

### Configurer les variables d'environnement

Configurez les variables d'environnement SQOOP_HOME et ajoutez également le sous-dossier bin dans la variable PATH .

Exécutez la commande suivante pour modifier le fichier .bashrc :

```
nano ~/.bashrc
```

Ajoutez les lignes suivantes à la fin du fichier :

```
export SQOOP_HOME=~/hadoop/sqoop-1.4.7.bin__hadoop-2.6.0                                         
export PATH=$SQOOP_HOME/bin:$PATH
```
Sourcez le fichier modifié pour le rendre effectif :
```
source ~/.bashrc
```
Modifier le fichier ```sqoop-env-template.sh``` en ajoutant les lignes suivantes :

```
export HADOOP_COMMON_HOME=~/hadoop/hadoop-3.3.0
export HADOOP_MAPRED_HOME=~/hadoop/hadoop-3.3.0
```

### Commons-lang / Mysql connector

Nous aurons besoin de ``` commons-lang-2.6.jar ``` , afin de convertir les donnees en UTF-8.

Dans le fichier ```/hadoop/sqoop-1.4.7/lib```, on va ajouter ```mysql-connector-java-5.1.48.jar``` et ```commons-lang-2.6.jar```

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/275d5550-58fd-4a4d-bb08-9de9427b804c)

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/ef6c8269-6c74-4598-9a28-bfdd956db27a)

### Print out version

``` 
sqoop version 
```
> Resultat devrait etre le suivant : Sqoop 1.4.7 

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/4c38fe0c-612d-4796-be0e-6f59913814a8)


## Creation de la base de donnees 

On lance ```XAMPP``` et on demarre ```Apache``` et ```Mysql```.

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/84fb8be5-64ae-4b3c-90b1-67a6c52a57f6)

on cree la base de donnee ``` db_sqoop``` qui contient la table ```customer```.

``` sql 
Create database db_sqoop;
Use db_sqoop;
CREATE TABLE `customer` (
 `ID` int(11) NOT NULL,
 `FirstName` varchar(60) NOT NULL,
 `LastName` varchar(60) NOT NULL,
 `CIN` varchar(60) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
ALTER TABLE `customer`
 ADD PRIMARY KEY (`ID`);
ALTER TABLE `customer`
 MODIFY `ID` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;
COMMIT;
```

On insere des donnees dans la table ```customer```

``` sql
INSERT INTO `customer` (`ID`, `FirstName`, `LastName`, `CIN`) VALUES
(1, 'daabal', 'sokaina', 'EE909090'),
(2, 'saad', 'sokaina', 'EE808080');
```

Dans phpMyadmin , on trouve ces resultat :

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/f8014237-b8f7-4a7d-a504-9b0f186e3dc0)

## Importer les donnees depuis Mysql vers HDFS 

nous avons besoin d'identifier l'adresse IP de la machine 

Pour identifier ip, on utlise la commande suivante :

```
ipconfig
```

on obtient la resulat suivante : 

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/60f0f9f1-7cae-4fb7-9830-e730161c4b70)


Donc l'adresse IP de notre base de donnees est ```192.168.56.1:3306```

pour importer les donnes de mysql vers hdfs on utilise la commande ```sqoop import``` :

``` sqoop
sqoop import --connect "jdbc:mysql://192.168.56.1:3306/db_sqoop" --username root --password '' --table customer --target-dir /user/hadoop/data/customer
```

Si tout passe en bon etat, on obtient les resulats suivant : 

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/39032ec5-1c3b-4a60-a6eb-ee64c126006e)

Pour acceder a hadoop a travers le lien suivant ```http://localhost:9870/```

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/b8aa7ae3-a424-45fb-b25e-352736c775b0)

## Importer les donnees depuis HDFS vers Mysql

On cree une liste des donnes dans un fichier texte.

```
nano customer 
```

Dans le fichier ```customer``` en ajout les enregistrements suivant :

```
3,Sokaina,Ahmed,EE707070
4,Saad,Ahmed,EE606060
5,Saad,mohammed,EE505050
6,Saad,sokaina,EE404040
```

Si le dossier source contient déjà des fichiers, on vide le dossier avant de transférer le fichier customer.txt.
```
$HADOOP_HOME/bin/hadoop fs -rm /user/hadoop/data/customer/*
```

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/53e24e3f-4019-42c2-ac19-52a786f26503)


on transfere le fichier ```customer``` vers HDFS.

```
$HADOOP_HOME/bin/hadoop fs -put customer /user/hadoop/data/customer
```

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/ee9a844b-7d23-4388-9347-27dfca18a39c)

On utilise la commande ```sqoop export ``` qui permet de charger les donnes depuis le HDFS et le stocker dans la base de donnee Mysql.

```
sqoop export --connect "jdbc:mysql://192.168.56.1:3306/db_sqoop" --username root --password '' --table customer --export-dir /user/hadoop/data/customer --input-fields-terminated-by ',' --input-lines-terminated-by '\n'
```
![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/c7d56690-d4a7-4d0b-822d-7073a3401a06)

On trouve les donnes ajouter dans la table ```customer```

![image](https://github.com/sokainadaabal/BigDataTPs/assets/48890714/f3e98e13-b63f-4dd0-8703-8d989739b762)
