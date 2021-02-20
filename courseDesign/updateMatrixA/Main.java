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
    public static void main(String[] args) throws Exception{
        // 各个输入输出文件的路径
        String resMatrixPath = "/project/rmatrix";
        String resDisCacheFile = "/project/sum/r-cache.txt";
        String avaMatrixPath = "/project/amatrix";
        String avaDisCacheFile = "/project/max";

        // *******************************assign availability matrix********************************
        Configuration assignAvaConfiguration = new Configuration();
        Job assignAvaJob = Job.getInstance(assignAvaConfiguration, "assignAva");

        assignAvaJob.addCacheFile(new Path(resDisCacheFile).toUri());

        assignAvaJob.setJarByClass(Main.class);
        assignAvaJob.setMapperClass(AssignAvaMapper.class);

        assignAvaJob.setMapOutputKeyClass(NullWritable.class);
        assignAvaJob.setMapOutputValueClass(Text.class);

        assignAvaJob.setOutputKeyClass(NullWritable.class);
        assignAvaJob.setOutputValueClass(Text.class);

        Path inputPath = new Path(resMatrixPath);
        Path outputPath = new Path(avaMatrixPath);
        FileSystem hdfs = FileSystem.get(assignAvaConfiguration);
        if(hdfs.exists(outputPath)){
            hdfs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(assignAvaJob, inputPath);
        FileOutputFormat.setOutputPath(assignAvaJob, outputPath);

        boolean ok = assignAvaJob.waitForCompletion(true);
        if(!ok) {
            System.out.println("assign availability matrix failed!");
        }

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
            System.out.println("assign availability matrix failed!");
        }

    }
}
