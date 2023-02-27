package ma.enset.Department;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DepartmentDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Configuration
        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        //les classes Mapper et Reducer
        job.setMapperClass(DepartmentMapper.class);
        job.setReducerClass(DepartmentReduce.class);
        //les types de sortie de la fonction map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        //les types de sortie du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //le format input
        job.setInputFormatClass(TextInputFormat.class);
        //le path des fichiers input/outpu
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}