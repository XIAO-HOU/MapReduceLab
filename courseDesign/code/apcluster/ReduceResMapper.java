package apcluster;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class ReduceResMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    LongWritable rowLong = new LongWritable();
    Text result = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("@");
        int row = Integer.parseInt(line[0]); // 得到行号

        // 得到每一行 rvalue@svalue的形式
        String[] elements = line[1].split(" ");

        int n = elements.length;
        // 以列号为键，行号和元素值为值输出，便于reduce阶段处理列的规约结果
        for (int col = 0; col < n; col++) {
            String element = elements[col];
            rowLong.set(col);
            result.set(row + "#" + element.split("#")[0]);
            context.write(rowLong, result);
        }
    }
}
