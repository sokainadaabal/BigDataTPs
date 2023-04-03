package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
public class VersionSQL {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");


        Dataset<Row> NbConsulatation = ss.read().format("jdbc")
                .options(options)
                .option("query","select DATE_CONSULTATION , count(*) as Nmbr_consultation from CONSULTATIONS GROUP BY  DATE_CONSULTATION ")
                .load();
        NbConsulatation.printSchema();
        NbConsulatation.show();
        Dataset<Row> NbConsulatationParMedecin = ss.read().format("jdbc")
                .options(options)
                .option("query","select med.NOM, med.PRENOM, count(*) as Nmbr_consultation from CONSULTATIONS cons JOIN MEDECINS med on med.ID=cons.ID_MEDECIN GROUP BY cons.DATE_CONSULTATION ")
                .load();
        NbConsulatationParMedecin.printSchema();
        NbConsulatationParMedecin.show();

        Dataset<Row> PatientByMedecin = ss.read().format("jdbc")
                .options(options)
                .option("query","select med.NOM as NomMedecin, count(*) as NBrPatient from CONSULTATIONS cons JOIN MEDECINS med on med.ID=cons.ID_MEDECIN JOIN PATIENTS pat on pat.ID=cons.ID_PATIENT GROUP BY cons.ID_PATIENT")
                .load();
        PatientByMedecin.printSchema();
        PatientByMedecin.show();
    }
}
