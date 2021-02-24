package apcluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProcessResultMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    final double INF = 1e9;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("@");
        // 得到行号
        int row = Integer.parseInt(line[0]);
        // 得到每个元素{avalue#svalue#rvalue}
        String[] data = line[1].split(" ");
        int n = data.length;
        double max = -INF;
        int pos = -1;       // id为row的数据应该属于哪个数据代表的类簇
        for(int col = 0; col < n; col++){
            String[] cur = data[col].split("#");
            // a[row][col]
            double a = Double.parseDouble(cur[0]);
            // r[row][col]
            double r = Double.parseDouble(cur[2]);
            // 更新pos
            if(a+r > max){
                max = a+r;
                pos = col;
            }
        }
        context.write(NullWritable.get(), new Text(row+" "+pos));
    }
}
