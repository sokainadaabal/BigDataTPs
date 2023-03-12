package ma.enset.Exercice2;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;

public class Exercice22 {
    private static final String NAMES_FILE = "ProduitVente.csv";
    public static void main(String[] args) throws IOException {
        SparkConf conf=new SparkConf();

        conf.setAppName("Exercuce 1").setMaster("local[*]"); // one process for  each process
        JavaSparkContext sc = new JavaSparkContext(conf);

        // recuperer les donnees a partir de fichier
        JavaRDD<String> RDD1= sc.textFile(NAMES_FILE);

        // Afficher le contenu de fichier recuperer
        RDD1.foreach((line)-> System.out.println(line));

        // supprimer l'entete de fichier
        JavaRDD<String> RDD2= RDD1.filter((line)-> !line.contains("ville"));

        // entete
        JavaRDD<String> RDD3=RDD1.filter((line)-> line.contains("ville")&& line.contains("prix"));

        // creation pair <ville ,prix>
        JavaPairRDD<String,Double> RDD4=RDD2.mapToPair((ventes)->{
            String[] ventesline=ventes.split(",");
            String ville= ventesline[0].trim().split("/")[0]+"_"+ventesline[1];
            Double prix=Double.valueOf(ventesline[3]);
            return  new Tuple2<>(ville,prix);
        });

        // calcule la somme de produit vendu dans une ville
        JavaPairRDD<String,Double> ventesParVille=RDD4.reduceByKey((prix,sum)->prix+sum);

        // creation de fichier de resultat

        File resulatFile= new File("exercice2-2.cvs");

        if(resulatFile.exists()) FileUtils.deleteQuietly(resulatFile);
        JavaRDD<String> ventes=ventesParVille.map((pair)->{
            return  pair._1()+","+pair._2();
        });
        FileUtils.writeLines(resulatFile,RDD3.union(ventes).collect());
    }
}
