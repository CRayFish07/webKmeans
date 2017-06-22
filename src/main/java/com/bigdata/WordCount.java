package com.bigdata;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



/**
 * Created by ivan on 6/16/17.
 * 提取网页中文部分、分词和去除停用词、统计词频
 * Map:
 * Input: 网页编号，网页内容
 * Output: 单词|网页编号，1
 * Reduce:
 * Input: 单词|网页编号，1
 * Output: 单词|网页编号，单词在网页编号出现的次数
 */
public class WordCount {

    public static class WordCountMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private static final Pattern ChinesePattern = Pattern.compile("[\u4e00-\u9fa5]");
        private Text word = new Text();
        private IntWritable count = new IntWritable(1);
        private JiebaSegmenter segmenter = new JiebaSegmenter();

        private static Set<String> stopwords = new HashSet<String>();
        final private static String stopwordPath = "./stopwords";


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //System.out.println(value.toString());
            //System.out.println("==============================================================");
            //获取停用词表
            if (stopwords.size() == 0) {
                File file = new File(stopwordPath);
                if (file.exists()) {
                    FileInputStream its = new FileInputStream(file);
                    InputStreamReader reader = new InputStreamReader(its);
                    BufferedReader bufferedReader = new BufferedReader(reader);
                    String stopword;
                    while ((stopword = bufferedReader.readLine()) != null)
                        stopwords.add(stopword);
                    its.close();
                }
            }




            //InputSplit split = context.getInputSplit();
            //String id = ((FileSplit)split).getPath().getName();
            String[] valueSplit = value.toString().split("@@@@@@@@@@");

            if (valueSplit.length < 2)
                return;

            String id = valueSplit[0];


            //获取网页中的中文
            StringBuilder chineseBuilder = new StringBuilder();
            Matcher m = ChinesePattern.matcher(value.toString());
            while (m.find()) {
                chineseBuilder.append(m.group());
            }
            //分词 去除停用词 统计词频
            List<SegToken> tokens = segmenter.process(chineseBuilder.toString(), SegMode.SEARCH);

            for (SegToken token : tokens){
                if(!stopwords.contains(token.word)) {
                    word.set(token.word + Tool.SEPARATOR + id);
                    context.write(word, count);
                }
            }


        }
    }

    public static class WordCountReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable wordCountSum = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values)
                sum = sum + value.get();
            wordCountSum.set(sum);
            context.write(key, wordCountSum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "-----------------------------------------------------");

        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path wordCountOutput = new Path(args[1]);
        if (fs.exists(wordCountOutput))
            fs.delete(wordCountOutput, true);
        FileOutputFormat.setOutputPath(job, wordCountOutput);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
