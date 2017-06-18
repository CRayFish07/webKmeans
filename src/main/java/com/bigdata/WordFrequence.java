package com.bigdata;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by ivan on 6/17/17.
 * 统计每个网页中的单词在网页总单词数的占比
 * Map:
 * Input: 单词|网页编号，单词在网页编号出现的次数
 * Output: 网页编号，单词|单词在网页编号出现的次数
 * Reduce:
 * Input: 网页编号，单词|单词在网页编号出现的次数
 * Output: 单词|网页编号， 单词在网页编号出现次数/网页的总单词数
 */
public class WordFrequence {
    final private static String SEPARATOR = "@";

    public static class WordFrequenceMapper extends
            Mapper<Text, IntWritable, Text, Text>{

        private Text pageId = new Text();
        private Text wordWordCount = new Text();

        protected void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException{
            String[] temp = key.toString().split(SEPARATOR);
            pageId.set(temp[1]);
            wordWordCount.set(temp[0] + SEPARATOR + value.toString());
            context.write(pageId, wordWordCount);
        }
    }

    public static class WordFrequenceReducer extends
            Reducer<Text, Text, Text, DoubleWritable>{

        private Text wordPageId = new Text();
        private DoubleWritable percent = new DoubleWritable();

        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            Map<String, Integer> counter = new HashMap<String, Integer>();
            int count = 0;
            for (Text wordWordCount : values){
                String[] temp = wordWordCount.toString().split(SEPARATOR);
                counter.put(temp[0], Integer.parseInt(temp[1]));
                count += Integer.parseInt(temp[1]);
            }

            for (String word : counter.keySet()){
                wordPageId.set(word + SEPARATOR + key.toString());
                percent.set(((double)counter.get(word))/count);
                context.write(wordPageId, percent);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordFrequence");

        job.setJarByClass(WordFrequence.class);
        job.setMapperClass(WordFrequenceMapper.class);
        job.setReducerClass(WordFrequenceReducer.class);

        //job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path wordFrequenceOutput = new Path(args[1]);
        if (fs.exists(wordFrequenceOutput))
            fs.delete(wordFrequenceOutput, true);
        FileOutputFormat.setOutputPath(job, wordFrequenceOutput);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
