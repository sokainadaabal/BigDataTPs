package ma.enset.VenteTotalesAnnee;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class VenteTotalesAnneeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // configuration
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf);
        //les classes Mapper et Reducer
        job.setMapperClass(VenteTotalesAnneeMapper.class);
        job.setReducerClass(VenteTotalesAnneeReduce.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //la format input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/output
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }
}
