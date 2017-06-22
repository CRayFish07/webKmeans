package com.bigdata;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by ivan on 6/17/17.
 * 统计单词出现的网页个数、TFIDF、建立词表
 * Map:
 * Input: 单词|网页编号， 单词在网页编号出现次数/网页的总单词数
 * Output: 单词，网页编号|单词在网页编号出现次数/网页的总单词
 * Reduce:
 * Input: 单词，网页编号|单词在网页编号出现次数/网页的总单词数
 * Output: 单词|网页编号，TFIDF值
 */
public class TFIDF {

    public static class TFIDFMapper extends
            Mapper<Text, DoubleWritable, Text, Text>{
        private Text word = new Text();
        private Text pageIdPercent = new Text();

        protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException{
            String[] temp = key.toString().split(Tool.SEPARATOR);
            word.set(temp[0]);
            pageIdPercent.set(temp[1] + Tool.SEPARATOR + value.toString());
            context.write(word, pageIdPercent);
        }
    }

    public static class TFIDFReducer extends
            Reducer<Text, Text, Text, DoubleWritable>{
        private Text wordPageId = new Text();
        private DoubleWritable tfidf = new DoubleWritable();
        private int numOfPage = 0;
        private static Map<String, Integer> wordDict = new HashMap<String, Integer>();
        private static int wordIndex = 0;

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException{
            //获取网页数量
            Configuration conf = context.getConfiguration();

            numOfPage = conf.getInt("numOfPage", -1);

            super.setup(context);

        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            int numOfPageWithWord = 0;
            Map<String, String> temp = new HashMap<String, String>();

            //建立词表
            if(!wordDict.containsKey(key.toString())){
                wordDict.put(key.toString(), wordIndex);
                wordIndex++;
            }

            for (Text value : values){
                String[] pageIdPercent = value.toString().split(Tool.SEPARATOR);
                temp.put(pageIdPercent[0], pageIdPercent[1]);
                if(Double.parseDouble(pageIdPercent[1]) > 0)
                    numOfPageWithWord++;
            }

            //计算TFIDF
            for (String pageId : temp.keySet()){
                double tf = Double.parseDouble(temp.get(pageId));


                double idf = 1 + Math.log10(((double)numOfPage) / (numOfPageWithWord + 1));
                //System.out.println(Double.toString(tf) +"  "+Double.toString(idf) + " " + numOfPage + " " + numOfPageWithWord);
                wordPageId.set(key.toString() + Tool.SEPARATOR + pageId);
                tfidf.set(tf * idf);
                context.write(wordPageId, tfidf);

            }
        }

        protected void cleanup(Context context)
            throws IOException, InterruptedException{
            //将字典保存
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            Path dictPath = new Path(conf.get("DICTPATH"));
            fs.delete(dictPath, true);


            final SequenceFile.Writer out =
                    SequenceFile.createWriter(fs, conf, dictPath, Text.class, IntWritable.class);


            Text word = new Text();
            IntWritable index = new IntWritable();

            for(String w : wordDict.keySet()){
                word.set(w);
                index.set(wordDict.get(w));
                out.append(word, index);
//                System.out.print(word);
//                System.out.println(index);
            }
            out.close();
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();


        conf.set("DICTPATH", args[2]);
        int numOfPage = 0;
        File[] pages = new File(args[3]).listFiles();
        if(pages != null)
            numOfPage = pages.length;
        conf.set("numOfPage", Integer.toString(numOfPage));

        Job job = Job.getInstance(conf, "TF-IDF");

        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //TFIDFInput, TFIDFOutput, DictPath, PageInput
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path TFIDFOutput = new Path(args[1]);
        fs.delete(TFIDFOutput, true);
        FileOutputFormat.setOutputPath(job, TFIDFOutput);



        System.exit(job.waitForCompletion(true)? 0 : 1);
    }


}
