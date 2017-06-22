package com.bigdata;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.bigdata.WordCount.*;
import com.bigdata.WordFrequence.*;
import com.bigdata.TFIDF.*;
import com.bigdata.BuildVector.*;
import com.bigdata.Kmeans.*;
import com.bigdata.KmeansResult.*;
/**
 * Created by ivan on 6/22/17.
 * 控制webKmeans的流程
 */
public class KmeansDriver {

    /**
     *
     * @param args : Input path, Output path, Temp path, K, maxIterations
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws
            IOException, InterruptedException, ClassNotFoundException{
        String inputPath = args[0];
        String outputPath = args[1];
        String tempPath = args[2];
        String dictPath = tempPath + "/dict";
        String centerPath = tempPath + "/center";
        String oldCenterPath = tempPath + "/oldCenter";

        int maxIteration = Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path wordCountOutput = new Path(tempPath + "/wordCountOutput");
        Path wordFrequenceOutput = new Path(tempPath + "/wordFrequenceOutput");
        Path tfidfOutput = new Path(tempPath + "/tfidfOutput");
        Path buildVectorOutput = new Path(tempPath + "/buildVectorOutput");
        Path kmeansOutput = new Path(tempPath + "/kmeansOutput");
        Path kmeansResultOutput = new Path(outputPath);

        conf.set("DICTPATH", dictPath);
        conf.set("CENTERPATH", centerPath);
        conf.set("OLDCENTERPATH", oldCenterPath);
        conf.set("K", args[3]);
        //网页总数 TODO
        conf.set("numOfPage", "2");
        conf.set("textinputformat.record.delimiter", "******************** separating line ********************");



        if (fs.exists(wordCountOutput))
            fs.delete(wordCountOutput, true);
        if (fs.exists(wordFrequenceOutput))
            fs.delete(wordFrequenceOutput, true);
        if (fs.exists(tfidfOutput))
            fs.delete(tfidfOutput, true);
        if (fs.exists(buildVectorOutput))
            fs.delete(buildVectorOutput, true);
        if (fs.exists(kmeansOutput))
            fs.delete(kmeansOutput, true);
        if (fs.exists(kmeansResultOutput))
            fs.delete(kmeansResultOutput, true);

        //提取网页中文部分、分词和去除停用词、统计词频
        Job wordCountJob = Job.getInstance(conf, "wordCount");
        wordCountJob.setJarByClass(WordCount.class);
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setReducerClass(WordCountReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        wordCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(wordCountJob, wordCountOutput);
        wordCountJob.waitForCompletion(true);

        //统计每个网页中的单词在网页总单词数的占比
        Job wordFrequenceJob = Job.getInstance(conf, "wordFrequence");
        wordFrequenceJob.setJarByClass(WordFrequence.class);
        wordFrequenceJob.setMapperClass(WordFrequenceMapper.class);
        wordFrequenceJob.setReducerClass(WordFrequenceReducer.class);
        wordFrequenceJob.setMapOutputKeyClass(IntWritable.class);
        wordFrequenceJob.setMapOutputValueClass(Text.class);
        wordFrequenceJob.setOutputKeyClass(Text.class);
        wordFrequenceJob.setOutputValueClass(DoubleWritable.class);
        wordFrequenceJob.setInputFormatClass(SequenceFileInputFormat.class);
        wordFrequenceJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(wordFrequenceJob, wordCountOutput);
        FileOutputFormat.setOutputPath(wordFrequenceJob, wordFrequenceOutput);
        wordFrequenceJob.waitForCompletion(true);

        //统计单词出现的网页个数、TFIDF、建立词表
        Job tfidfJob = Job.getInstance(conf, "TFIDF");
        tfidfJob.setJarByClass(TFIDF.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setInputFormatClass(SequenceFileInputFormat.class);
        tfidfJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        tfidfJob.setMapOutputKeyClass(Text.class);
        tfidfJob.setMapOutputValueClass(Text.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(tfidfJob, wordFrequenceOutput);
        FileOutputFormat.setOutputPath(tfidfJob, tfidfOutput);
        tfidfJob.waitForCompletion(true);

        //构造网页向量、随机选取初始中心点
        Job buildVectorJob = Job.getInstance(conf, "buildVector");
        buildVectorJob.setJarByClass(BuildVector.class);
        buildVectorJob.setMapperClass(BuildVectorMapper.class);
        buildVectorJob.setReducerClass(BuildVectorReducer.class);
        buildVectorJob.setInputFormatClass(SequenceFileInputFormat.class);
        buildVectorJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        buildVectorJob.setMapOutputKeyClass(IntWritable.class);
        buildVectorJob.setMapOutputValueClass(Text.class);
        buildVectorJob.setOutputKeyClass(IntWritable.class);
        buildVectorJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(buildVectorJob, tfidfOutput);
        FileOutputFormat.setOutputPath(buildVectorJob, buildVectorOutput);
        buildVectorJob.waitForCompletion(true);

        for (int i = 0; i < maxIteration; i++){
            //实现Kmeans、迭代求解直至满足条件
            System.out.println("Clustering...." + " " + i);

            Job kmeansJob = Job.getInstance(conf, "Kmeans");
            kmeansJob.setJarByClass(Kmeans.class);
            kmeansJob.setMapperClass(KmeansMapper.class);
            kmeansJob.setReducerClass(KmeansReducer.class);
            kmeansJob.setInputFormatClass(SequenceFileInputFormat.class);
            kmeansJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            kmeansJob.setMapOutputKeyClass(IntWritable.class);
            kmeansJob.setOutputValueClass(Text.class);
            kmeansJob.setOutputKeyClass(IntWritable.class);
            kmeansJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(kmeansJob, buildVectorOutput);
            if (fs.exists(kmeansOutput))
                fs.delete(kmeansOutput, true);
            FileOutputFormat.setOutputPath(kmeansJob, kmeansOutput);
            kmeansJob.waitForCompletion(true);

            //簇心不变，聚类结束
            if (!Tool.centersChange(fs, conf, oldCenterPath, centerPath))
                break;


        }

        //输出每个网页对应的编号
        Job kmeansResultJob = Job.getInstance(conf, "Kmeans result");
        kmeansResultJob.setJarByClass(KmeansResult.class);
        kmeansResultJob.setMapperClass(KmeansResultMapper.class);
        kmeansResultJob.setReducerClass(KmeansResultReducer.class);
        kmeansResultJob.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        kmeansResultJob.setMapOutputKeyClass(IntWritable.class);
        kmeansResultJob.setOutputValueClass(IntWritable.class);
        kmeansResultJob.setOutputKeyClass(IntWritable.class);
        kmeansResultJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(kmeansResultJob, buildVectorOutput);
        FileOutputFormat.setOutputPath(kmeansResultJob, kmeansResultOutput);
        kmeansResultJob.waitForCompletion(true);





    }
}
