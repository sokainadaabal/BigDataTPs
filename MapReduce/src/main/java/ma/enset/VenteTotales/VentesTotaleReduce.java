package ma.enset.VenteTotales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class VentesTotaleReduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text keyVente, Iterable<DoubleWritable> totaleVentes, Context context) throws IOException, InterruptedException {
        // calcule le total vendu dans chaque ville
        Iterator<DoubleWritable> iterator = totaleVentes.iterator();
        double totale = 0;
        while(iterator.hasNext()){
            totale+=iterator.next().get();
        }
        System.out.println(" ville :" + keyVente+"****** Prix :"+ totale);
        context.write(keyVente,new DoubleWritable(totale));
    }
}
