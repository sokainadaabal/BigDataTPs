package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DepartmentMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        String[] department = value.toString().split(";");

        context.write(new Text(department[2]),new FloatWritable(Float.valueOf(department[4])));
    }
}
