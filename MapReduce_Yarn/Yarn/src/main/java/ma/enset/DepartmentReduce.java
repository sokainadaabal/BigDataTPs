package ma.enset;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DepartmentReduce extends Reducer<Text, FloatWritable,Text,Text> {

}
