import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.StringTokenizer;

/**
 * Created by nastya on 02.01.16.
 */

class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String stringValue = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(stringValue," \n\t\r");
        String word;
        LongWritable one = new LongWritable(1);
        while(tokenizer.hasMoreTokens()) {
            word = normalize(tokenizer.nextToken());
            if (word != null) {
                context.write(new Text(word), one);
            }
        }
    }
    private String normalize(String word) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < word.length(); i++) {
            char letter = word.charAt(i);
            if (Character.isLetter(letter)) {
                builder.append(letter);
            }
        }
        String newWord = builder.toString().toLowerCase();
        if (newWord.length() == 0 || newWord.charAt(0) != 'n') {
            return null;
        } else {
            return newWord;
        }
    }
}

class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalValue = 0;
        for (LongWritable value : values) {
            totalValue += value.get();
        }
        context.write(key, new LongWritable(totalValue));
    }
}

public class WordCount {
    public static void main(String[] args) {
        Job job = null;
        try {
            job = Job.getInstance(new Configuration(),"word count");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return;
        }
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return;
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        try {
            job.waitForCompletion(false);
        } catch (IOException  | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}