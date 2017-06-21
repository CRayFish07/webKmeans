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

    /**
     * 将tfidfs转为Map<Integer, Double>
     * @param tfidfs 单词编号1|TFIDF值1&单词编号2|TFIDF值2...
     * @return
     */
    protected static Map<Integer, Double> str2map(Text tfidfs){
        Map<Integer, Double> wordTfidf = new HashMap<Integer, Double>();
        String[] wts = tfidfs.toString().split(AND);
        for (String wt :wts){
            wordTfidf.put(Integer.parseInt(wt.split(SEPARATOR)[0]),
                    Double.parseDouble(wt.split(SEPARATOR)[1]));
        }
        return wordTfidf;
    }

    public static class KmeansMapper extends
            Mapper<IntWritable, Text, IntWritable, Text>{
        private IntWritable corrCenter = new IntWritable();
        private Map<Integer, Map<Integer, Double>> centers = new HashMap<Integer, Map<Integer, Double>>();
        private Map<String, Integer> wordDict = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            //读取词表
            Path dictPath = new Path(conf.get("DICTPATH"));
            SequenceFile.Reader dictReader = new SequenceFile.Reader(fs, dictPath, conf);
            Text word = new Text();
            IntWritable index = new IntWritable();
            while (dictReader.next(word, index))
                wordDict.put(word.toString(), index.get());
            dictReader.close();

            //读取中心
            Path centerPath = new Path(conf.get("CENTERPATH"));
            SequenceFile.Reader centerReader = new SequenceFile.Reader(fs, centerPath, conf);
            IntWritable centerId = new IntWritable();
            Text tfidfs = new Text();
            while (centerReader.next(centerId, tfidfs)){
                Map<Integer, Double> wordTFIDF = str2map(tfidfs);
                centers.put(centerId.get(), wordTFIDF);
            }
            centerReader.close();

            super.setup(context);
        }

        protected void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException{
            int corrC = getNearestNeighbour(str2map(value));
            corrCenter.set(corrC);
            context.write(corrCenter, value);
        }

        /**
         * 找出与改样本距离最近的中心
         * @param page
         * @return
         */
        private int getNearestNeighbour(Map<Integer, Double> page){
            int index = 0;
            double miniDist = 0.0;
            for(int i : centers.keySet()){
                double dist = getDistance(centers.get(i), page);
                if (dist < miniDist){
                    miniDist = dist;
                    index = i;
                }
            }
            return index;
        }

        /**
         * 计算该网页与中心的余弦距离
         * @param center
         * @param page
         * @return
         */
        private double getDistance(Map<Integer, Double> center, Map<Integer, Double> page){
            double sum = 0;
            double sum1 = 0;
            double sum2 = 0;
            for(int i = 0; i < wordDict.size(); i++){
                if (center.containsKey(i) && page.containsKey(i)) {
                    sum += (center.get(i) * page.get(i));
                    sum1 += Math.pow(center.get(i), 2);
                    sum2 += Math.pow(page.get(i), 2);
                }else if (center.containsKey(i)){
                    sum1 += Math.pow(center.get(i), 2);
                }else if (page.containsKey(i)){
                    sum2 += Math.pow(page.get(i), 2);
                }

            }
            return sum / (Math.sqrt(sum1) * Math.sqrt(sum2));
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
            FileSystem fs = FileSystem.get(conf);
            //读取词表
            Path dictPath = new Path(conf.get("DICTPATH"));
            SequenceFile.Reader dictReader = new SequenceFile.Reader(fs, dictPath, conf);
            Text word = new Text();
            IntWritable index = new IntWritable();
            while (dictReader.next(word, index))
                wordDict.put(word.toString(), index.get());
            dictReader.close();

            //将上次迭代后的中心(CENTER)保存到(OLDCENTER)
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
                Map<Integer, Double> page = str2map(value);
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
