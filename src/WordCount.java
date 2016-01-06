import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

class ComplexKey implements WritableComparable<ComplexKey> {
    private Text word;
    private LongWritable count;

    public ComplexKey() {
        word = new Text();
        count = new LongWritable();
    }

    public ComplexKey(Text newWord, LongWritable newCount) {
        this();
        word.set(newWord);
        count.set(newCount.get());
    }
    public Text getWord() {
        return word;
    }
    public LongWritable getCount() {
        return count;
    }
    @Override
    public int compareTo(ComplexKey o) {
        int wordResult = word.compareTo(o.word);
        int countResult = count.compareTo(o.count);
        if (countResult == 0) {
            return wordResult;
        } else {
            return -countResult;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        count.readFields(dataInput);
    }
}

class YetAnotherMapper extends Mapper<Object, Text, ComplexKey, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] pair = value.toString().split("\\s+");
        Text word = new Text(pair[0]);
        LongWritable count = new LongWritable(Long.parseLong(pair[1]));
        context.write(new ComplexKey(word, count),count);
    }
}

class YetAnotherReducer extends Reducer<ComplexKey, LongWritable, Text, LongWritable>  {

    @Override
    protected void reduce(ComplexKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key.getWord(), key.getCount());
    }
}


public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job firstJob = Job.getInstance(new Configuration(),"word count");
        Job secondJob = Job.getInstance(new Configuration(), "sort");

        firstJob.setMapperClass(MyMapper.class);
        firstJob.setReducerClass(MyReducer.class);
        secondJob.setMapperClass(YetAnotherMapper.class);
        secondJob.setReducerClass(YetAnotherReducer.class);

        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path("tmp"));
        FileInputFormat.addInputPath(secondJob, new Path("tmp"));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));

        firstJob.setInputFormatClass(TextInputFormat.class);
        firstJob.setOutputFormatClass(TextOutputFormat.class);
        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);

        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(LongWritable.class);
        secondJob.setOutputKeyClass(ComplexKey.class);
        secondJob.setOutputValueClass(LongWritable.class);

        firstJob.setJarByClass(WordCount.class);
        secondJob.setJarByClass(WordCount.class);

        firstJob.waitForCompletion(false);
        secondJob.waitForCompletion(false);

    }
}