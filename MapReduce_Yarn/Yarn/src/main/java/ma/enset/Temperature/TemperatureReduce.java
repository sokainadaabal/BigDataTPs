package ma.enset.Temperature;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TemperatureReduce  extends Reducer<Text, DoubleWritable,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTemp=Double.MIN_VALUE,minTemp=Double.MAX_VALUE;
            Iterator<DoubleWritable> val=values.iterator();
            val.next();
            if(val.hasNext()){
                double currentTemp=  val.next().get() ;
                if(maxTemp<currentTemp) maxTemp = currentTemp;
                if(minTemp>currentTemp) minTemp = currentTemp;
            }
            context.write(new Text(key),new Text("min : "+minTemp +" , max: "+maxTemp));
    }
    
}
