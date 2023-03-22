package ma.enset.StreamingWordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf sparkConf=new SparkConf().setAppName("TP 1 Spark Streaming").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(sparkConf, new Duration(5000));
        JavaDStream<String> javaReceiverInputDStream=javaStreamingContext.textFileStream("./data/names");
        JavaDStream<String> javaDStream=javaReceiverInputDStream.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> javaPairDStream=javaDStream.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> javaPairDStream1=javaPairDStream.reduceByKey((a, b) -> a+b);
        javaPairDStream1.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}