package ma.enset.Department;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DepartmentReduce extends Reducer<Text, FloatWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
        Iterator<FloatWritable> iterator = values.iterator();
        float max=iterator.next().get();
        float min=max;
        
        while(iterator.hasNext()){
            float val=iterator.next().get();
            max=(max<val)?val:max;
            min=(min>val)?val:min;
        }
        context.write(new Text(key),new Text(" max : "+max+" , min : "+min));
    }
}
