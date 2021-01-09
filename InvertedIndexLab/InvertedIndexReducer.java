package invertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    String preWord;
    List<String> postingList;
    int sum;
    int tot;

    // 去掉文件名不需要的后缀
    public String changeFileName(String fileName){
        int len = fileName.length();
        // 记录倒数第二个点号的位置
        int p = 0;
        // 记录倒数第一个点号是否已经出现
        boolean ok = false;
        for(int i = len-1; i >= 0; i--){
            if(fileName.charAt(i) == '.'){
                if(ok){
                    p = i;
                    break;
                }
                ok = true;
            }
        }
        String newFileName = fileName.substring(0, p);
        return newFileName;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        preWord = "";
        postingList = new LinkedList<String>();
        sum = 0;
        tot = 0;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String word = key.toString().split("=")[0];
        String doc = changeFileName(key.toString().split("=")[1]);
        int fre = 0;
        Iterator<IntWritable> itValues = values.iterator();
        while(itValues.hasNext()){
            fre += itValues.next().get();
        }
        if(word.equals(preWord)){
            postingList.add(doc+":"+fre);
            sum += fre;
            tot += 1;
        }else{
            if(tot > 0) {
                double avg = 1.0 * sum / tot;
                String postings = String.format("%.2f", avg);
                Iterator<String> it = postingList.iterator();
                boolean ok = false;
                while (it.hasNext()) {
                    if (!ok) {
                        postings = postings + "," + it.next();
                        ok = true;
                    } else {
                        postings = postings + ";" + it.next();
                    }
                }
                context.write(new Text(preWord), new Text(postings));
            }
            preWord = word;
            postingList.clear();
            postingList.add(doc+":"+fre);
            sum = fre;
            tot = 1;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(tot > 0){
            double avg = 1.0 * sum / tot;
            String postings = String.format("%.2f", avg);
            Iterator<String> it = postingList.iterator();
            boolean ok = false;
            while(it.hasNext()){
                if(!ok){
                    postings = postings + "," + it.next();
                    ok = true;
                }else {
                    postings = postings + ";" + it.next();
                }
            }
            context.write(new Text(preWord), new Text(postings));
        }
    }
}
