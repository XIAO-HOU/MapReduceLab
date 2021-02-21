package apcluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ReduceResReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
    Text result = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        double diagonal = 0.0;

        // 列号
        long col = Long.parseLong(key.toString());

        for (Text value: values) {
            // 获取行号和值
            String[] elements = value.toString().split("#");
            long row = Long.parseLong(elements[0]);
            double val = Double.parseDouble(elements[1]);
            if (row == col) {
                diagonal = val;
            }
            val = Double.max(0.0, val);
            sum += val;
        }
        result.set(col + "@" + sum + "#" + diagonal);
        context.write(NullWritable.get(), result);
    }

}
