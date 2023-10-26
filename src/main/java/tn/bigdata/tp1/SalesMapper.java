package tn.bigdata.tp1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text store = new Text();
    private IntWritable cost = new IntWritable();

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e) {
            return false;
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 4 && isNumeric(tokens[4])) {
            store.set(tokens[2]);
            cost.set((int) Double.parseDouble(tokens[4]));
            context.write(store, cost);
        }
    }

}
