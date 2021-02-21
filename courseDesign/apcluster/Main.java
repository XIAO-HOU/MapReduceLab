package apcluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    // 各个输入输出文件的路径
    public static final String resMatrixPath = "/project/rmatrix";
    public static final String resDisCacheFile = "/project/sum";
    public static final String avaMatrixPath = "/project/amatrix";
    public static final String avaDisCacheFile = "/project/max";
    public static final String cacheFile = "/part-r-00000";

    // 阻尼系数
    public static final Double K = 0.5;

    public static void main(String[] args) throws Exception{
        Path inputPath;     // 输入文件路径
        Path outputPath;    // 输出文件路径
        FileSystem hdfs;    // hdfs
        boolean ok;         // mapreduce任务是否完成

        // *******************************calculate max(a + r) for each row********************************
        Configuration calMaxConfiguration = new Configuration();
        Job calMaxJob = Job.getInstance(calMaxConfiguration, "calMax");

        calMaxJob.setJarByClass(Main.class);
        calMaxJob.setMapperClass(CalMaxMapper.class);
        calMaxJob.setReducerClass(CalMaxReducer.class);

        calMaxJob.setMapOutputKeyClass(IntWritable.class);
        calMaxJob.setMapOutputValueClass(Text.class);

        calMaxJob.setOutputKeyClass(NullWritable.class);
        calMaxJob.setOutputValueClass(Text.class);

        inputPath = new Path(avaMatrixPath);
        outputPath = new Path(avaDisCacheFile);
        hdfs = FileSystem.get(calMaxConfiguration);
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(calMaxJob, inputPath);
        FileOutputFormat.setOutputPath(calMaxJob, outputPath);

        ok = calMaxJob.waitForCompletion(true);
        if(!ok) {
            System.out.println("calculate mas(a+r) failed!\n");
        }

        // *******************************assign responsibility matrix********************************
        Configuration assignResConfiguration = new Configuration();
        Job assignResJob = Job.getInstance(assignResConfiguration, "assignRes");

        assignResJob.addCacheFile(new Path(avaDisCacheFile+cacheFile).toUri());

        assignResJob.setJarByClass(Main.class);
        assignResJob.setMapperClass(AssignResMapper.class);

        assignResJob.setMapOutputKeyClass(NullWritable.class);
        assignResJob.setMapOutputValueClass(Text.class);

        assignResJob.setOutputKeyClass(NullWritable.class);
        assignResJob.setOutputValueClass(Text.class);

        inputPath = new Path(avaMatrixPath);
        outputPath = new Path(resMatrixPath);
        hdfs = FileSystem.get(assignResConfiguration);
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(assignResJob, inputPath);
        FileOutputFormat.setOutputPath(assignResJob, outputPath);

        ok = assignResJob.waitForCompletion(true);
        if(!ok) {
            System.out.println("assign responsibility matrix failed!\n");
        }

        // *******************************calculate sum {max(0, r)} for each col********************************
        Configuration calSumConfiguration = new Configuration();
        Job calSumJob = Job.getInstance(calSumConfiguration, "calSum");

        calSumJob.setJarByClass(Main.class);
        calSumJob.setMapperClass(ReduceResMapper.class);
        calSumJob.setReducerClass(ReduceResReducer.class);

        calSumJob.setMapOutputKeyClass(LongWritable.class);
        calSumJob.setMapOutputValueClass(Text.class);

        calSumJob.setOutputKeyClass(NullWritable.class);
        calSumJob.setOutputValueClass(Text.class);

        inputPath = new Path(resMatrixPath);
        outputPath = new Path(resDisCacheFile);
        hdfs = FileSystem.get(calSumConfiguration);
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(calSumJob, inputPath);
        FileOutputFormat.setOutputPath(calSumJob, outputPath);

        ok = calSumJob.waitForCompletion(true);
        if(!ok) {
            System.out.println("calculate sum responsibility matrix failed!\n");
        }

        // *******************************assign availability matrix********************************
        Configuration assignAvaConfiguration = new Configuration();
        Job assignAvaJob = Job.getInstance(assignAvaConfiguration, "assignAva");

        assignAvaJob.addCacheFile(new Path(resDisCacheFile+cacheFile).toUri());

        assignAvaJob.setJarByClass(Main.class);
        assignAvaJob.setMapperClass(AssignAvaMapper.class);

        assignAvaJob.setMapOutputKeyClass(NullWritable.class);
        assignAvaJob.setMapOutputValueClass(Text.class);

        assignAvaJob.setOutputKeyClass(NullWritable.class);
        assignAvaJob.setOutputValueClass(Text.class);

        inputPath = new Path(resMatrixPath);
        outputPath = new Path(avaMatrixPath);
        hdfs = FileSystem.get(assignAvaConfiguration);
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(assignAvaJob, inputPath);
        FileOutputFormat.setOutputPath(assignAvaJob, outputPath);

        ok = assignAvaJob.waitForCompletion(true);
        if(!ok) {
            System.out.println("assign availability matrix failed!\n");
        }

    }
}
