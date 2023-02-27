package ma.enset.DepartmentTotaleEmploye;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DepartmentTotaleReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            Iterator<IntWritable> iterator = values.iterator();
            int somme=0;


            while(iterator.hasNext()){
                somme+=iterator.next().get();
            }

            context.write(new Text(key), new IntWritable(somme));

        }

}
