package tn.bigdata.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        System.out.println(key.toString() + " " + sum);
        result.set((int) sum);
        context.write(key, result);
    }
}


