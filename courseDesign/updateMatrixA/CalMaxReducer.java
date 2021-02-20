package apcluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CalMaxReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    final Double INF = 1e9+7;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double max = -INF;
        double secMax = -INF;
        double pos = -1;
        int tot = 0;
        for (Text value : values) {
            // cur[0] = "a+s"，和的值
            // cur[1] = "col"，所处的列
            String[] cur = value.toString().split("#");
            double sumValue = Double.parseDouble(cur[0]);
            int col = Integer.parseInt(cur[1]);
            // 更新最大值，次大值以及最大值所处的列位置
            if (sumValue > max) {
                secMax = max;
                max = sumValue;
                pos = col;
            } else if (sumValue > secMax) {
                secMax = sumValue;
            }
            tot += 1;
        }
        // 如果样本数只有1，为了不影响后续计算，secMax设为0
        if(tot <= 1){
            secMax = 0;
        }
        String result = key.get()+"@"+max+"#"+secMax+"#"+pos;
        context.write(NullWritable.get(), new Text(result));
    }
}
