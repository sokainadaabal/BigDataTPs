package ma.enset.Temperature;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        /**
         * "STATION", "DATE", "SOURCE",	"LATITUDE",	"LONGITUDE", "ELEVATION", "NAME", "REPORT_TYPE",
         * "CALL_SIGN","QUALITY_CONTROL", "WND", "CIG",	"VIS",	"TMP", "DEW", "SLP", "AA1",	"AY1", "GF1", "IA1", "MD1",	"MW1", "EQD"
         *  annuler le  retenir de la ligne 1
         */
            //annuler le  retenir de la ligne 1
            if(key.get()==0 & value.toString().contains("STATION")) return;
            String[] dataline = value.toString().trim().split("\",\"");
            String date= dataline[1];
            // la temperature
            String  temp = dataline[13];
            // annee et la temperature
            String anneeMonth=date.split("-")[0].concat("-"+ date.split("-")[1]);
            double  temperator = Double.valueOf(temp.replace(",","."));
            context.write(new Text(anneeMonth),new DoubleWritable(temperator));
    }
}
