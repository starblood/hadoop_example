import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/*
    TODO: 1. 컴바이너 적용하기
    TODO: 2. 카운터를 적용하여 총 단어 수와 유일한 단어 수를 계산하기
    TODO: 3. 리듀스 태스크의 수를 2개로 지정하기
    TODO: 4. 출력 포맷으로 SequenceFileOutputFormat 을 사용하기
    TODO: 5. 최종결과 파일을 HDFS에서 로컬 파일 시스템으로 다운로드하여 내용보기
 */
public class WordCount {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n\f |,.()<>");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sumWritable = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            sumWritable.set(sum);

            context.getCounter("WordStat", "number of unique words").increment(1);
            context.getCounter("WordStat", "number of total words").increment(sum);

            context.write(key, sumWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setCombinerClass(MyReducer.class);

        job.setNumReduceTasks(2); // set number of reduce tasks

        // if mapper outputs are different, call setMapOutputKeyClass and setMapOutputValueClass
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // An InputFormat for plain text files. Files are broken into lines. Either linefeed or carriage-return are used to signal end of line.
        // Keys are the position in the file, and values are the line of text..
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
