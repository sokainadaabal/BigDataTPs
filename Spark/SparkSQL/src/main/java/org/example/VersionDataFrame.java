package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class VersionDataFrame {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

         // query 1
        Dataset<Row> df1=ss.read().format("jdbc").option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
        .option("user","root").option("password","").option("dbtable","CONSULTATIONS").load();
        df1.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count().show();

        // query 2
        Dataset<Row> df2_1=ss.read().format("jdbc").option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user","root").option("password","").option("dbtable","CONSULTATIONS").load();
        Dataset<Row> df2_2=ss.read().format("jdbc").option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("user","root").option("password","").option("dbtable","MEDECINS").load();
        Dataset<Row> jointure1=df2_1.join(df2_2);
        jointure1.printSchema();
        jointure1.show();
        jointure1.select("NOm","PRENOM").groupBy("NOM","PRENOM").count().show();
        // query 3


    }
}
