package dataJoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;

import java.net.URI;
import java.util.Hashtable;

public class DataJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Hashtable<String, String> joinData = new Hashtable<>();

    @Override
    protected void setup(Context context) {
        try{
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            URI[] cacheFiles = context.getCacheFiles();

            if(cacheFiles != null && cacheFiles.length > 0){
                Path filePath = new Path(cacheFiles[0]);
                String line;
                String[] tokens;
                BufferedReader joinReader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
                try{
                    while((line = joinReader.readLine()) != null){
                        tokens = line.split(" ", 2);
                        joinData.put(tokens[0], tokens[1]);
                    }
                }finally{
                    joinReader.close();
                }
            }
        }catch(IOException e){
            System.err.println("Exception reading DistributedCache:"+e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] items;
        items = value.toString().split(" ");
        String productId = items[2];
        if(joinData.containsKey(productId)) {
            String joinValue = joinData.get(productId);
            context.write(NullWritable.get(), new Text(String.format("%s %s %s %s %s", items[0], items[1], items[2], joinValue, items[3])));
        }
    }
}
