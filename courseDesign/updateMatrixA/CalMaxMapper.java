package apcluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CalMaxMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 用"@"分割得到行号和该行对应的值
        String[] line = value.toString().split("@");
        int row = Integer.parseInt(line[0]);

        // 得到每一行（"avalue#svalue"）形式的列表
        String[] elements = line[1].split(" ");

        int n = elements.length;
        for(int col = 0; col < n; col++){
            String[] cur = elements[col].split("#");
            double a = Double.parseDouble(cur[0]);
            double s = Double.parseDouble(cur[1]);
            double sum = a + s;
            String result = sum + "#" + col;
            context.write(new IntWritable(row), new Text(result));
        }
    }
}
