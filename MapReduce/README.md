<table>
  <tr >
    <th style="text-align: center;">Ecole Normale Supérieure de l’Enseignement  Technique Mohammedia Université Hassan II de Casablanca</th>
    <th><img src="https://www.clubs-etudiants.ma/wp-content/uploads/2018/11/enst-1.png"/></th>
    <th style="text-align: center;"> Département Mathématiques et Informatique « Ingénierie Informatique : Big Data t Cloud Computing » II-BDCC2   </th>
  </tr>
</table>
          <table>
            <tr >
              <th style="text-align: center;">Réaliser par </th>
              <th style="text-align: center;">  Module  </th>
              <th style="text-align: center;">Filière</th>
              <th style="text-align: center;">Date </th>
              <th style="text-align: center;">  E-mail  </th>
              <th style="text-align: center;">  Doc  </th>
            </tr>
            <tr >
              <td>Daabal Sokaina</td>
              <td> Big data</td>
              <td> II-BDCC 2 </td>
              <td> 27 FEVRIER 2023 </td>
              <td> sokainadaabal@gmail.com / s.daabal@etu.enset-media.ac.ma  </td>
              <td> C.Rendu </td>
            </tr>
          </table> 


# TP  1 : MapReduce & Yarn 
## 1. Introduction
> Dans ce TP, nous avons la possibilité de voir de quelle façon un fichier .txt peut être traité. Et retourner un résultat avec MapReduce.
## 2.	Objectif de TP1
-	Traiter le fichier des ventes.
-	Retourner le total des ventes dans chacune des villes.
-	Retourner le total des ventes de chaque ville à une année déterminée.
## 3.	Réalisation 
Réaliser ce TP, fait cela dans une machine virtuelle, nous avons déjà installé en elle Hadoop et configurer hdfs et yarn.
 ### 3.1. Préparation de l’environnement 
 #### 3.1.1. Formater du système de fichiers hdfs 
Pour formater le système de fichiers, il faut exécuter la commande suivante :
```     
    hadoop namenode -format
```    
#### 3.1.2.	Démarrage de Hadoop  
Il existe deux manières de lancer le système Hadoop, soit en exécutant cette commande :
 ``` 
    stat-all.sh
 ```
Ou démarrer Hadoop avec la commande suivante :
```
    start-dfs.sh
```
Et par la suite démarrer yarn, avec l’exécution de commande suivante :
 ```
    start-yarn.sh
 ```
#### 3.1.3.	Vérification le fonctionnement de Hadoop 
Pour vérifier si Hadoop fonctionne bien, entrez la commande jps et vous obtiendrez le résultat suivant :
```
    $ jps 
    16128 Jps 
    15297 ResourceManager
    15430 NodeManager
    13943 NameNode
    14474 SecondaryNameNode
    14093 DataNode
```
### 3.2.	MapReduce
#### 3.2.1	Création du fichier ventes.txt 
Nous entamons notre TP, par la création du fichier `ventes.txt` dans le système de fichiers Hadoop, ce fichier contiendra les données suivantes :
```
2023/02/19 casa hp 20938.00
2022/02/02 casa hp 1980.00
2023/02/18 casa lenovo 23846.00
2019/12/10 jadida dell 1500.00
```
#### 3.2.2 Exécution d’un job MapReduce 
##### 3.2.2.1	Compter le nombre de vente par ville
> Pour compter le nombre de ventes par ville, nous allons écrire un job MapReduce qui va d'abord lire le fichier `ventes.txt` et enverra un mapper qui contient la clé qui présente la ville et la valeur qui présente le prix du produit vendu. 
Reduce comptabilisera les montants du prix du produit vendu et donnera un résultat en fichier.
Pour réaliser ce traitement, nous avons créé deux classes :
- `VentesTotaleMapper.java`
``` java
  public class VentesTotaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
     @Override
      protected void map(LongWritable key, Text line,Context context) throws IOException, 
        InterruptedException {
          // chaque line contient ces valeurs date ville produit prix
          // en veut retourner la ville et le prix de produit vendu
          String[] ListVente = line.toString().toLowerCase().trim().split(" ");
          System.out.println(" ville :" + ListVente[1] +"****** Prix :"+ListVente[3]);
          // key et value retourner par Mapper
          String keyVille = ListVente[1];
          double valuePrix = Double.valueOf(ListVente[3]);

          context.write(new Text(keyVille),new DoubleWritable(valuePrix));
         }
       }
```         
- `VentesTotaleReduce.java`
``` java 
  public class VentesTotaleReduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
        @Override
        protected void reduce(Text keyVente, Iterable<DoubleWritable> totaleVentes, Context context) throws IOException,
          InterruptedException {
            // calcule le total vendu dans chaque ville
            Iterator<DoubleWritable> iterator = totaleVentes.iterator();
            double totale = 0;
            while(iterator.hasNext()){
                totale+=iterator.next().get();
            }
            System.out.println(" ville :" + keyVente+"****** Prix :"+ totale);
            context.write(keyVente,new DoubleWritable(totale));
         }
       }
```
- `Pour lancer ce traitement, nous allons créer un job` 
``` java 
  public class VentesTotaleDriver  {

      public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
          // configuration
          Configuration conf = new Configuration();
          Job job=Job.getInstance(conf);
          //les classes Mapper et Reducer
          job.setMapperClass(VentesTotaleMapper.class);
          job.setReducerClass(VentesTotaleReduce.class);
          //les types de sortie de la fonction map
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(DoubleWritable.class);
          //les types de sortie du job
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(DoubleWritable.class);
          //le format input
          job.setInputFormatClass(TextInputFormat.class);
          //le path des fichiers input/output
          FileInputFormat.addInputPath(job,new Path(args[0]));
          FileOutputFormat.setOutputPath(job,new Path(args[1]));
          job.waitForCompletion(true);
      }
  }
```
Pour exécuter le job MapReduce. Il est nécessaire de modifier la configuration dans IntellIJ pour générer un fichier .jar.
Ensuite, nous allons exécuter cette commande : 
```         
hadoop jar MapReduce-1.0-SNAPSHOT.jar ma/enset/venteTotales/VenteTotalesDriver /user/root/BigDataTPs/MapReduce/ventes.txt /user/root/BigDataTPs/MapReduce/input
```
Pour afficher le résultat du MapReduce, on va exécuter la commande suivante suivante :
```
$ hdfs dfs -cat /user/root/BigDataTPs/MapReduce/input/part-r-00000
casa    46764.0
jadida  1500.0    
```
##### 3.2.2.2	Compter le nombre de ventes par ville pendant une année donnée
> Calculer le prix de vente total des marchandises par ville au cours d'une année donnée. on écrira un job MapReduce qui lira en premier le fichier ventes.txt. et enverra un mapper qui contient la clé qui présente la ville concaténée par la date et la valeur qui présente le prix du produit vendu à la date indiquée. Reduce comptabilisera les montants du prix du produit vendu durant une année et donnera un résultat en fichier.
Pour réaliser ce traitement, nous avons créé deux classes :
- `VenteTotalesAnneeMapper.java`
``` java
public class VenteTotalesAnneeMapper extends Mapper<LongWritable, Text, Text , DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        // chaque line contient ces valeurs date ville produit prix
        // en veut retourner la ville et le prix de produit vendu
        String[] ListVente = line.toString().toLowerCase().trim().split(" ");
        System.out.println(" ville :" + ListVente[1] +"****** Prix :"+ListVente[3]);
         // retour date
        String dateIn = ListVente[0];
        String yearIn= dateIn.split("/")[0];
        // associer la ville avec la date
        // key et value retourner par le mapper
        String keyVille = ListVente[1].concat("-").concat(yearIn);
        double valuePrix = Double.valueOf(ListVente[3]);
        context.write(new Text(keyVille),new DoubleWritable(valuePrix));
    }
}
````
- `VentesTotalesAnneeReduce.java`
``` java
public class VenteTotalesAnneeReduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text keyVente, Iterable<DoubleWritable> totaleVentes, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // calcule le total vendu dans chaque ville dans une année
        Iterator<DoubleWritable> iterator = totaleVentes.iterator();
        double totale = 0;
        while(iterator.hasNext()){
            totale+=iterator.next().get();
        }
        System.out.println(" ville :" + keyVente+"****** Prix :"+ totale);
        context.write(keyVente,new DoubleWritable(totale));
    }
}
````
- `VenteTotalesAnneeDriver.java`
``` java
public class VenteTotalesAnneeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // configuration
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf);
        //les classes Mapper et Reducer
        job.setMapperClass(VenteTotalesAnneeMapper.class);
        job.setReducerClass(VenteTotalesAnneeReduce.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //la format input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/output
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}

````
Pour exécuter le job MapReduce. Il est nécessaire de modifier la configuration dans IntellIJ pour générer un fichier .jar.
Ensuite, nous allons exécuter cette commande : 
```         
hadoop jar MapReduce-1.0-SNAPSHOT.jar ma/enset/VenteTotalesAnnee/VenteTotalesAnneeDriver /user/root/BigDataTPs/MapReduce/ventes.txt /user/root/BigDataTPs/MapReduce/inputTotale
```
Pour afficher le résultat du MapReduce, on va exécuter la commande suivante suivante :
```
$ hdfs dfs -cat /user/root/BigDataTPs/MapReduce/inputTotale/part-r-00000
casa-2022    1980.0
casa-2023    44784.0
jadida-2019  1500.0    
```
## 4. Conclusion
> Ce travail m'a donné l'opportunité de savoir manipuler un fichier qui contient un certain nombre de données. et a ensuite retourné les informations souhaitées avec une garantie de fiabilité et de réduire le temps de prendre des décisions ou d'obtenir une constatation.


> __Note__

- $\color{orange}{Vous \ pouvez \ visualiser \ les \ captures \ d'écran \  de \ l'exécution \ du \ code \ on \ cliquant \ sur }$  [lien](https://github.com/sokainadaabal/BigDataTPs/tree/main/MapReduce/Captures) 
