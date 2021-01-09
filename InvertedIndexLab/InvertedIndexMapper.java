package invertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, Integer> tokenFrequency = new HashMap<String, Integer>();

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while(stringTokenizer.hasMoreTokens()){
            String word = stringTokenizer.nextToken();
            int f = tokenFrequency.getOrDefault(word, 0);
            tokenFrequency.put(word, f + 1);
        }

        // 获取所在文件文件名
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        Text posting = new Text();
        IntWritable frequency = new IntWritable();
        for(Map.Entry<String, Integer> entry : tokenFrequency.entrySet()){
            posting.set(entry.getKey() + "=" + fileName);
            frequency.set(entry.getValue());
            context.write(posting, frequency);
        }
    }
}
