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

import com.bigdata.Tool;

/**
 * Created by ivan on 6/21/17.
 * 输出每个网页对应的编号
 * Map:
 * Input: 网页编号，单词编号1|TFIDF值1&单词编号2|TFIDF值2..
 * Output:　网页编号，中心编号
 * Reduce:
 * Input:　网页编号，中心编号
 * Output:　网页编号，中心编号
 */
public class KmeansResult {
    public static class KmeansResultMapper extends
            Mapper<IntWritable, Text, IntWritable, IntWritable>{

        private Map<Integer, Map<Integer, Double>> centers = new HashMap<Integer, Map<Integer, Double>>();
        private Map<String, Integer> wordDict = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            //读取词表
            wordDict = Tool.readWordDict(conf, conf.get("DICTPATH"));
            //读取中心
            centers = Tool.readCenter(conf, conf.get("CENTERPATH"));

        }


        protected void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            int corrCenter = Tool.getNearestNeighbour(Tool.text2map(value), centers, wordDict.size());
            context.write(key, new IntWritable(corrCenter));
        }
    }

    public static class KmeansResultReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            for (IntWritable val : values){
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args)
        throws Exception{
        //KmeansResultInput, KmeansResultOutput, CENTERPATH, DICTPATH
        Configuration conf = new Configuration();
        conf.set("CENTERPATH", args[2]);
        conf.set("DICTPATH", args[3]);

        Job job = Job.getInstance(conf, "Kmeans result");
        job.setJarByClass(KmeansResult.class);
        job.setMapperClass(KmeansResultMapper.class);
        job.setReducerClass(KmeansResultReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path KmeansResultOutput = new Path(args[1]);
        fs.delete(KmeansResultOutput, true);
        FileOutputFormat.setOutputPath(job, KmeansResultOutput);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
