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

public class AssignResMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    final int N = 100000;
    double[] max1 = new double[N];  // 每行最大值
    double[] max2 = new double[N];  // 每行次大值
    int[] colIndex = new int[N];    // 最大值所处的列

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取distributedCache中的文件
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path filePath = new Path(cacheFiles[0]);
            String line;
            String[] element;
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
            try {
                while ((line = reader.readLine()) != null) {
                    // line
                    String[] value =  line.split("@");
                    // 获得行号
                    int row = Integer.parseInt(value[0]);
                    // 得到最大值和次大值以及最大值的列索引
                    element = value[1].split("#");
                    max1[row] = Double.parseDouble(element[0]);
                    max2[row] = Double.parseDouble(element[1]);
                    colIndex[row] = Integer.parseInt(element[2]);
                }
            } finally {
                reader.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 用"@"分割得到行号和该行对应的值
        String[] line = value.toString().split("@");

        // 行号
        int row = Integer.parseInt(line[0]);

        // 得到每一行("avalue#svalue")形式的列表
        String[] elements = line[1].split(" ");

        int n = elements.length;
        String result = row + "@";
        for (int col = 0; col < n; col++) {
            String[] cur = elements[col].split("#");
            double a = Double.parseDouble(cur[0]);
            double s = Double.parseDouble(cur[1]);
            double r_old = Double.parseDouble(cur[2]);
            double r_new;
            // 依据a矩阵更新公式进行更新
            if (col != colIndex[row]) {
                r_new = s - max1[row];
            } else {
                r_new = s - max2[row];
            }
            // 阻尼系数防止数据在某个范围内发生振荡，不收敛
            double r = Main.K * r_old + (1 - Main.K) * r_new;
            result += r+"#"+s+"#"+a;
            if (col < n - 1) {
                result += " ";
            }
        }

        context.write(NullWritable.get(), new Text(result));
    }
}
