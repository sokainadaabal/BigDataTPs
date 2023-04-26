<table>
  <tr >
    <th style="text-align: center;">Ecole Normale Supérieure de l’Enseignement  Technique Mohammedia Université Hassan II de Casablanca</th>
    <th><img src="https://user-images.githubusercontent.com/48890714/230882470-f070fc71-e968-4785-855d-ebe3d44fe672.png"/></th>
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
              <th style="text-align: center;"> Type  </th>
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


# TP  2 : Yarn
## Introduction
Le système de traitement de données distribué Apache Hadoop a été conçu pour gérer efficacement de grands ensembles de données sur des clusters de serveurs. Hadoop est un écosystème de logiciels open-source qui comprend de nombreux modules tels que HDFS (Hadoop Distributed File System) pour stocker et répartir les données, et YARN (Yet Another Resource Negotiator) pour planifier et allouer les ressources de traitement. Le but de ce TP est de comprendre comment utiliser YARN pour gérer les ressources de traitement et exécuter des applications MapReduce.
## Objectif de ce TP
 - traiter un fichier de type csv.
 - retourne des résultats selon un programme suivi.
## Réalisation 
### Execrcice 1.
#### le fichier employees.csv 

```
ahmed;slimani;informatique;ingenieur;20000.0
saad;tazi;finance;gestionnaire;17000.0
hajar;saadani;informatique;manager;24000.0
karim;rmili;finance;comptable;9000.0
khaoula;naji;informatique;ingenieur;20000.0
```
> Les noms de colonnes : firstName,lastName,department, jobTitle,salary.
#### Problème à résoudre
1. Étant donné une liste d'employés avec leur département et leur salaire, trouvez le salaire maximum et minimum dans chaque département.
Pour résoudre ce probléme, nous avons créé deux classes
 - ```DepartmentMapper.java```
 ``` java
 public class DepartmentMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) 
      throws IOException, InterruptedException {
        String[] department = value.toString().split(";");

        context.write(new Text(department[2]),new FloatWritable(Float.valueOf(department[4])));
    }
}
 ```
 - ```DepartmentReduce.java```
 ``` java
 public class DepartmentReduce extends Reducer<Text, FloatWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
        Iterator<FloatWritable> iterator = values.iterator();
        float max=iterator.next().get();
        float min=max;
        
        while(iterator.hasNext()){
            float val=iterator.next().get();
            max=(max<val)?val:max;
            min=(min>val)?val:min;
        }
        context.write(new Text(key),new Text(" max : "+max+" , min : "+min));
    }
}
 ```
Pour lancer ce traitement, nous allons créer un job
 - ```DepartmentDriver.java```
 ``` java
 public class DepartmentDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Configuration
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        //les classes Mapper et Reducer
        job.setMapperClass(DepartmentMapper.class);
        job.setReducerClass(DepartmentReduce.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //le format input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/outpu
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

    }
}
 ```
Pour exécuter le job MapReduce. Il est nécessaire de modifier la configuration dans IntellIJ pour générer un fichier .jar.
Ensuite, nous allons exécuter cette commande :
```
hadoop jar Yarn-1.0-SNAPSHOT.jar ma/enset/Department/DepartmentDriver /user/root/BigDataTPs/Yarn/Employeurs.csv /user/root/BigDataTPs/Yarn/output/
```

Pour afficher le résultat du MapReduce, on va exécuter la commande suivante :

````
$ hdfs dfs -cat /user/root/BigDataTPs/Yarn/output/part-r-00000
finance	max :17000.0 , min :9000.0
informatique	max :24000.0 , min :20000.0
````
2. Étant donné une liste d'employés avec leur département, trouvez le nombre d'employés dans chaque département.

Pour réaliser ce traitement, nous avons créé deux classes
 - ```DepartmentTotaleMapper.java```
 ``` java
 public class DepartmentTotaleMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] department = value.toString().split(";");

        context.write(new Text(department[2]),new IntWritable(1));
    }
}
 ```
 - ```DepartmentTotaleReduce.java```
 ``` java
 public class DepartmentTotaleReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Iterator<IntWritable> iterator = values.iterator();
            int somme=0;


            while(iterator.hasNext()){
                somme+=iterator.next().get();
            }

            context.write(new Text(key), new IntWritable(somme));
        }
}  
 ```
Pour lancer ce traitement, nous allons créer un job
 - ```DepartmentTotaleDriver.java```
 ``` java
public class DepartmentTotaleDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Configuration
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        //les classes Mapper et Reducer
        job.setMapperClass(DepartmentTotaleMapper.class);
        job.setReducerClass(DepartmentTotaleReduce.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //le format input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/outpu
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
 ```
Pour exécuter le job MapReduce. Il est nécessaire de modifier la configuration dans IntellIJ pour générer un fichier .jar.
Ensuite, nous allons exécuter cette commande :
```
hadoop jar Yarn-1.0-SNAPSHOT.jar ma/enset/DepartmentTotaleEmploye/DepartmentTotaleDriver /user/root/BigDataTPs/Yarn/Employeurs.csv /user/root/BigDataTPs/Yarn/outputTotale
```
Pour afficher le résultat du MapReduce, on va exécuter la commande suivante :

```
$ hdfs dfs -cat /user/root/BigDataTPs/Yarn/outputTotale/part-r-00000
finance	2
informatique	3
```
### Exercice 2.
#### [le fichier de température file1.csv](https://www.ncei.noaa.gov/data/global-hourly/archive/csv/)
#### Probléme à résoudre
Extraire les valeurs de température et calculer la température minimale et maximale pour chaque année.
Pour résoudre ce probléme, nous avons créé deux classes
 - ```TemperatureMappet.java```
``` java
public class TemperatureMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) 
    throws IOException, InterruptedException {
        /**
         * "STATION", "DATE", "SOURCE",	"LATITUDE",	"LONGITUDE", "ELEVATION", "NAME", "REPORT_TYPE",
         * "CALL_SIGN","QUALITY_CONTROL","WND","CIG","VIS","TMP", "DEW","SLP","AA1","AY1","GF1","IA1","MD1","MW1","EQD"
         *  annuler le  retenir de la ligne 1
         */
            //annuler le  retenir de la ligne 1
            if(key.get()==0 & value.toString().contains("STATION")) return;
            String[] dataline = value.toString().trim().split("\",\"");
            String date= dataline[1];
            // la temperature
            String  temp = dataline[13];
            // annee et la temperature
            String anneeMonth=date.trim().split("-")[0].concat("-"+ date.split("-")[1]);
            double  temperator = Double.valueOf(temp.replace(",","."));
            context.write(new Text(anneeMonth),new DoubleWritable(temperator));
    }
}
 ```
 - ```TemperatureReduce.java```
 ``` java
 public class TemperatureReduce  extends Reducer<Text, DoubleWritable,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTemp=Double.MIN_VALUE,minTemp=Double.MAX_VALUE;
            Iterator<DoubleWritable> val=values.iterator();
            val.next();
            if(val.hasNext()){
                double currentTemp=  val.next().get() ;
                if(maxTemp<currentTemp) maxTemp = currentTemp;
                if(minTemp>currentTemp) minTemp = currentTemp;
            }
            context.write(new Text(key),new Text("min : "+minTemp +" , max: "+maxTemp));
    }
    
}
 ```
Pour lancer ce traitement, nous allons créer un job
 - ```TemperatureDriver.java```
 ``` java
 public class TemperatureDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       // Configuration
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);

        //les classes Mapper et Reducer
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReduce.class);

        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //le format input
        job.setInputFormatClass(TextInputFormat.class);

        //le path des fichiers input/outpu
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // execute job
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
 ```
Pour exécuter le job MapReduce. Il est nécessaire de modifier la configuration dans IntellIJ pour générer un fichier .jar.
Ensuite, nous allons exécuter cette commande :
```
hadoop jar Yarn-1.0-SNAPSHOT.jar ma/enset/DepartmentTotaleEmploye/TemperatureDriver /user/root/BigDataTPs/Yarn/file1.csv /user/root/BigDataTPs/Yarn/outputFile
```
Pour afficher le résultat du MapReduce, on va exécuter la commande suivante :

```
hdfs dfs -cat /user/root/BigDataTPs/Yarn/outputFile/part-r-00000
1984-01 min : 5.1 , max: 5.1
1984-02 min : -32.1 , max: 4.9E-324
1984-03 min : 48.1 , max: 48.1
1984-04 min : 50.1 , max: 50.1
1984-05 min : 10.1 , max: 10.1
1984-06 min : 102.1 , max: 102.1
1984-07 min : 99.1 , max: 99.1
1984-08 min : 167.1 , max: 167.1
1984-10 min : 1.7976931348623157E308 , max: 4.9E-324
```

## Conclusion 
En conclusion, le système YARN est un composant clé de l'écosystème Hadoop qui permet la gestion efficace des ressources de traitement dans un environnement distribué. En utilisant YARN, nous pouvons facilement exécuter des programmes MapReduce sur un cluster Hadoop et profiter de la mise à l'échelle horizontale pour gérer de grands ensembles de données. Bien qu'il y ait une certaine courbe d'apprentissage pour travailler avec YARN, il s'agit d'un outil puissant pour les ingénieurs en données qui cherchent à gérer des charges de travail de données distribuées à grande échelle.
