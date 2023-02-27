package ma.enset.VenteTotales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class VentesTotaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text line,Context context) throws IOException, InterruptedException {
        // chaque line contient ces valeurs date ville produit prix
        // en veut retourner la ville et le prix de produit vendu
        String[] ListVente = line.toString().toLowerCase().trim().split(" ");
        System.out.println(" ville :" + ListVente[1] +"****** Prix :"+ListVente[3]);
        // key et value retourner par Mapper
        String keyVille = ListVente[1];
        double valuePrix = Double.valueOf(ListVente[3]);

        context.write(new Text(keyVille),new DoubleWritable(valuePrix));
    }
}
