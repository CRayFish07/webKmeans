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
 * Created by ivan on 6/19/17.
 * 实现Kmeans、迭代求解直至满足条件
 * Map:
 * Input: 网页编号，单词编号1|TFIDF值1&单词编号2|TFIDF值2..
 * Output: 网页对应中心点编号，单词编号1|TFIDF值1&单词编号2|TFIDF值2...
 * Reduce:
 * Input: 网页对应中心点编号，单词编号1|TFIDF值1&单词编号2|TFIDF值2...
 * Output: 网页对应中心点编号，新的中心点向量
 */
public class Kmeans {
    final private static String SEPARATOR = "@";
    final private static String AND = "&";



    public static class KmeansMapper extends
            Mapper<IntWritable, Text, IntWritable, Text>{
        private IntWritable corrCenter = new IntWritable();
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

            super.setup(context);
        }

        protected void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            int corrC = Tool.getNearestNeighbour(Tool.text2map(value), centers, wordDict.size());
            corrCenter.set(corrC);
            context.write(corrCenter, value);
        }

    }


    public static class KmeansReducer extends
            Reducer<IntWritable, Text, IntWritable, Text>{
        private Map<String, Integer> wordDict = new HashMap<String, Integer>();
        private Map<Integer, String> centerTfidf = new HashMap<Integer, String>();
        private Map<Integer, String> oldCenterTfidf = new HashMap<Integer, String>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            //读取词表
            wordDict = Tool.readWordDict(conf, conf.get("DICTPATH"));

            //将上次迭代后的中心(CENTER)保存到(OLDCENTER)
            FileSystem fs = FileSystem.get(conf);
            Path centerPath = new Path(conf.get("CENTERPATH"));
            SequenceFile.Reader oldCenterReader = new SequenceFile.Reader(fs, centerPath, conf);
            Path oldCenterPath = new Path(conf.get("OLDCENTERPATH"));
            fs.delete(oldCenterPath);
            SequenceFile.Writer oldCenterWriter
                    = new SequenceFile.Writer(fs, conf, oldCenterPath, IntWritable.class, Text.class);
            IntWritable i = new IntWritable();
            Text tfidfs = new Text();
            while (oldCenterReader.next(i, tfidfs)) {
                oldCenterWriter.append(i, tfidfs);
                oldCenterTfidf.put(i.get(), tfidfs.toString());
            }
            oldCenterReader.close();
            oldCenterWriter.close();


            super.setup(context);
        }

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            //计算新的簇心
            int num = 0;
            double[] newCenter = new double[wordDict.size()];

            for (Text value : values){
                Map<Integer, Double> page = Tool.text2map(value);
                for (int i : page.keySet()){
                    newCenter[i] += page.get(i);

                }
                num++;
            }


            StringBuilder newCenterBuilder = new StringBuilder();

            for (int i = 0; i < wordDict.size(); i++) {

                newCenter[i] = newCenter[i] / num;

                newCenterBuilder.append(i);
                newCenterBuilder.append(SEPARATOR);
                newCenterBuilder.append(newCenter[i]);
                newCenterBuilder.append(AND);

            }

            centerTfidf.put(key.get(), newCenterBuilder.toString());
            context.write(key, new Text(newCenterBuilder.toString()));
        }


        @Override
        protected void cleanup(Context context)
            throws IOException, InterruptedException{
            for(int i : oldCenterTfidf.keySet()){
                if(!centerTfidf.containsKey(i)){
                    context.write(new IntWritable(i), new Text(oldCenterTfidf.get(i)));
                    centerTfidf.put(i, oldCenterTfidf.get(i));
                }
            }

            //把新的簇心存到CENTERPATH
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            Path centerPath = new Path(conf.get("CENTERPATH"));
            fs.delete(centerPath, true);
            SequenceFile.Writer centerWriter
                    = new SequenceFile.Writer(fs, conf, centerPath, IntWritable.class, Text.class);

            for (int i : centerTfidf.keySet()){
                centerWriter.append(new IntWritable(i), new Text(centerTfidf.get(i)));
            }
            centerWriter.close();

        }



    }



    public static void main(String[] args)
        throws Exception{
        //KmeansInput, KmeansOutput, CENTERPATH, OLDCENTERPATH, DICTPATH
        Configuration conf = new Configuration();
        conf.set("CENTERPATH", args[2]);
        conf.set("OLDCENTERPATH", args[3]);
        conf.set("DICTPATH", args[4]);

        Job job = Job.getInstance(conf, "Kmeans");

        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path KmeansOutput = new Path(args[1]);
        fs.delete(KmeansOutput, true);
        FileOutputFormat.setOutputPath(job, KmeansOutput);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
