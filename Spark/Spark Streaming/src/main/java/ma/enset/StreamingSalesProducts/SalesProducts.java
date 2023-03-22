package ma.enset.StreamingSalesProducts;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SalesProducts {
    public static void main(String[] args) throws InterruptedException  {
        SparkConf sparkConf=new SparkConf().setAppName("TP 1 Spark Streaming").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(sparkConf, new Duration(5000));
        JavaDStream<String> javaReceiverInputDStream=javaStreamingContext.textFileStream("./data/produitVente");
        JavaDStream<String> javaDStream=javaReceiverInputDStream.filter((line)->!line.contains("ville"));

        JavaPairDStream<String,Double> javaPairDStream=javaDStream.mapToPair((ventes)->{
            String[] vente=ventes.split(",");
            String ville=vente[1];
            Double prix=Double.parseDouble(vente[3]);
            return new Tuple2<>(ville,prix);
        });

        JavaPairDStream<String,Double>  javaPairDStream1=javaPairDStream.reduceByKey((a,b)-> a+b );
        javaPairDStream1.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
