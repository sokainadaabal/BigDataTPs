package ma.enset.Exercice3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;



public class Exercice3 {

    private static final String NAMES_FILE = "Temperateur_2020.csv";
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();

        conf.setAppName("Exercuce 3").setMaster("local[*]"); // one process for  each process
        JavaSparkContext sc = new JavaSparkContext(conf);

        // recuperer les donnees a partir de fichier
        JavaRDD<String> RDD1= sc.textFile(NAMES_FILE);
        // Temperature et type
        JavaPairRDD<String,Double> RDD2=RDD1.mapToPair((lineLu)->{
            String[] line=lineLu.split(",");
            return  new Tuple2<>(line[2],Double.valueOf(line[3]));
        });
        // Calucle la moyenne minimale
        JavaPairRDD<String,Double> RDD4=RDD2.filter(x->x._1.equals("TMIN"));
        double tailleMoyen=RDD4.count();
        // regouper les temps par type
        JavaPairRDD<String,Double> RDD3=RDD4.reduceByKey((tem,sum)->tem+sum);
        JavaPairRDD<String,Double> tempMinNumber = RDD3.mapValues(x-> x/tailleMoyen);
        System.out.println("Température minimale moyenne: "+ tempMinNumber.collect());
        // Calucle la moyenne maximale
        JavaPairRDD<String,Double> RDD5=RDD2.filter(x->x._1.equals("TMAX"));
        double tailleMax=RDD5.count();
        // regouper les temps par type
        JavaPairRDD<String,Double> RDD6=RDD5.reduceByKey((tem,sum)->tem+sum);
        JavaPairRDD<String,Double> tempMaxNumber = RDD6.mapValues(x-> x/tailleMax);
        System.out.println("Température maximale moyenne: "+ tempMaxNumber.collect());

        // la temperature minimale
        JavaPairRDD<String,Double> Tmin =RDD4.reduceByKey((x,y)->Math.min(x,y));
        System.out.println("La temperature minimale "+Tmin.collect() );

        // la temperature maximale
        JavaPairRDD<String,Double> Tmax=RDD5.reduceByKey((x,y)->Math.max(x,y));
        System.out.println("La temperature maximale "+Tmax.collect() );
        // les 5 top temperature minimale
        JavaRDD<Tuple3<String,String,Double>> topMeto= RDD1.map(ligne->new Tuple3<>(ligne.split(",")[0],ligne.split(",")[2],Double.parseDouble(ligne.split(",")[3])));

        JavaPairRDD<String,Double> topMeto_1=topMeto.filter(x->x._2().equals("TMIN")).mapToPair(ligne->new Tuple2<>(ligne._1(),ligne._3()));

        JavaPairRDD<String,Double> topMeto_2=topMeto_1.reduceByKey((x,y)->Math.min(x,y));
        JavaPairRDD<String,Double> topMeto_3=topMeto_2.mapToPair(ligne->new Tuple2<>(ligne._1(), ligne._2()));
        JavaPairRDD<String,Double> topMeto_4= topMeto_3.sortByKey(false);

        System.out.println("les stations les plus froid \n" + topMeto_4.take(5));

        // les 5 top temperature maximale
        JavaPairRDD<String,Double> topMetomax_1=topMeto.filter(x->x._2().equals("TMAX")).mapToPair(ligne->new Tuple2<>(ligne._1(),ligne._3()));

        JavaPairRDD<String,Double> topMetomax_2=topMetomax_1.reduceByKey(((x,y)->Math.max(x,y)));
        JavaPairRDD<String,Double> topMetomax_3=topMetomax_2.mapToPair(ligne->new Tuple2<>(ligne._1(), ligne._2()));
        JavaPairRDD<String,Double> topMetomax_4= topMetomax_3.sortByKey(true);

        System.out.println("Les stations les plus champs\n "+topMetomax_4.take(5));


    }
}
