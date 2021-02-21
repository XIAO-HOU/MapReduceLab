package apcluster;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class AssignAvaMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    final int N = 100000;
    double[] sum = new double[N];
    double[] diagonal = new double[N];

    @Override
    protected void setup(Context context) throws IOException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        URI[] cacheFiles = context.getCacheFiles();

        if(cacheFiles != null && cacheFiles.length > 0){
            Path filePath = new Path(cacheFiles[0]);
            String line;
            String[] element;
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
            try{
                while((line = reader.readLine()) != null){
                    // line{行号@列的和#矩阵r对角线值}
                    String[] value = line.split("@");
                    // 得到列号
                    int col = Integer.parseInt(value[0]);

                    // 得到列的和以及矩阵r对角线值
                    element = value[1].split("#");
                    sum[col] = Double.parseDouble(element[0]);
                    diagonal[col] = Double.parseDouble(element[1]);
                }
            }finally {
                reader.close();
            }
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 用"@"分割得到行号和该行对应的值
        String[] line = value.toString().split("@");
        int row = Integer.parseInt(line[0]); // 得到行号

        // 得到每一行（"rvalue#svalue"）形式的列表
        String[] elements = line[1].split(" ");

        int n = elements.length;
        String result = row+"@";
        for(int col = 0; col < n; col++){
            // cur[0] = "rvalue"
            // cur[1] = "svalue"
            String[] cur = elements[col].split("#");
            double r = Double.parseDouble(cur[0]); // r[row][col]
            double s = Double.parseDouble(cur[1]); // s[row][col]
            double a_old = Double.parseDouble(cur[2]);
            double a_new = 0.0;
            if(row == col) {
                a_new = sum[col] - Math.max(0, r);
            }else{
                a_new = Math.min(0, diagonal[col] + sum[col] - Math.max(0, r) - Math.max(0, diagonal[col]));
            }
            double a = Main.K * a_old + (1 - Main.K) * a_new;
            result += a+"#"+s+"#"+r;
            if(col < n-1){
                result += " ";
            }
        }
        context.write(NullWritable.get(), new Text(result));
    }
}
