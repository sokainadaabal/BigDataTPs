package ma.enset.VenteTotalesAnnee;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class VenteTotalesAnneeReduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text keyVente, Iterable<DoubleWritable> totaleVentes, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // calcule le total vendu dans chaque ville dans une année
        Iterator<DoubleWritable> iterator = totaleVentes.iterator();
        double totale = 0;
        while(iterator.hasNext()){
            totale+=iterator.next().get();
        }
        System.out.println(" ville :" + keyVente+"****** Prix :"+ totale);
        context.write(keyVente,new DoubleWritable(totale));
    }
}
