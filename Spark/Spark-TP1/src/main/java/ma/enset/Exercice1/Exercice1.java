package ma.enset.Exercice1;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;

public class Exercice1 {
    private static final String NAMES_FILE = "names.csv";
    public static void main(String[] args)  throws IOException {
        SparkConf conf=new SparkConf();

        conf.setAppName("Exercuce 1").setMaster("local[*]"); // one process for  each process
        JavaSparkContext sc = new JavaSparkContext(conf);

        // creer une list des noms des etudiants c'est une list de string
        List<String> nameList= FileUtils.readLines(new File(NAMES_FILE), Charset.defaultCharset());

        //RDD

        // prallize
        JavaRDD<String> rdd1 = sc.parallelize(nameList);

        // flatMap
        JavaRDD<String>  rdd2= rdd1.flatMap(s-> Arrays.asList(s.concat(" BDCC")).iterator());
        System.out.println("RDD2 ; utilisation flatmap ");
        rdd2.foreach((name)-> System.out.println(name));
        // filter :
        JavaRDD<String> rdd3=rdd2.filter((e)->e.startsWith("S")|e.startsWith("s"));
        JavaRDD<String> rdd4=rdd2.filter((e)->e.length()>4);
        JavaRDD<String> rdd5=rdd2.filter((e)->e.contains("n"));
        System.out.println("RDD3 ; Les noms commence par S | s ");
        rdd3.foreach((name)-> System.out.println(name));
        System.out.println("RDD4 ; Les noms avec size superieure de 4");
        rdd4.foreach((name)-> System.out.println(name));
        System.out.println("RDD5 ; Les noms qui contient lq lettre n");
        rdd5.foreach((name)-> System.out.println(name));
        JavaRDD<String> rdd6= rdd3.union(rdd4);
        System.out.println(" RDD 6 contient les valeur de RDD4 et RDD5");
        rdd6.foreach((name)-> System.out.println(name));
        // ajouter superfixe a le nom
        // si un char ascii < (65+24)/2 add Mr
        // else add ms
        JavaRDD<String> rdd71=rdd5.map((name)->{
            int center =60+10;
            int firstcaractere =name.toString().toUpperCase().charAt(0);
            if(center<firstcaractere) return "mr. ".concat(name.toString());
            else return "ms. ".concat(name.toString());
        });
        System.out.println("Les noms avec superfix");
        rdd71.foreach((name)-> System.out.println(name));
        JavaRDD<String> rdd81=rdd6.map((name)->name.toString().toLowerCase());
        System.out.println("Les noms majusacile");
        rdd81.foreach((name)-> System.out.println(name));

        //mapToPaire
        JavaPairRDD<String,Integer> rdd72= rdd71.mapToPair((name)->new Tuple2<>(name,1));
        System.out.println("le cle et la valeur de nom");
        rdd72.foreach((name)-> System.out.println(name));
        JavaPairRDD<String,Integer> rdd82 = rdd81.mapToPair((name)->new Tuple2<>(name,1));
        System.out.println("le cle et la valeur de nom");
        rdd82.foreach((name)-> System.out.println(name));
        // reduceByKey
        JavaPairRDD<String,Integer> rdd7= rdd72.reduceByKey((paireKey,sum)-> sum+paireKey);
        System.out.println("RDD7 ; le nombre de mot");
        rdd7.foreach((name)-> System.out.println(name));
        JavaPairRDD<String,Integer> rdd8 = rdd82.reduceByKey((paireKey,sum)-> sum+paireKey);
        System.out.println("RDD8 ; le nombre de mot");
        rdd8.foreach((name)-> System.out.println(name));
        // map
        JavaRDD<String> rdd73= rdd7.map((pair)-> pair.toString().toLowerCase());


        JavaRDD<String> rdd83 = rdd8.map((pair)-> pair.toString().toLowerCase());

        JavaRDD<String> rdd9 = rdd83.union(rdd73);
        System.out.println("les nom RDD9");
        rdd9.foreach((name)-> System.out.println(name));
        JavaRDD<String> rdd10=rdd9.sortBy((name)->name,true,1);

        System.out.println(" RDD 10");
        rdd10.foreach((name)-> System.out.println(name));
        System.out.println("################################## fin ############################");
    }
}