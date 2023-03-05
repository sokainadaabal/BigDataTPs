package ma.enset.Temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;

public class TemperatureDriver {
    public static void main(String[] args) throws Exception {
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
