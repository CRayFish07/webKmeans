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
 * Created by ivan on 6/18/17.
 * 构造网页向量、随机选取初始中心点
 * Map:
 * Input: 单词|网页编号，TFIDF值
 * Output: 网页编号，单词|TFIDF值
 * Reduce:
 * Input: 网页编号，单词|TFIDF值
 * Output: 网页编号，单词编号1|TFIDF值1&单词编号2|TFIDF值2..
 */
public class BuildVector {

    public static class BuildVectorMapper extends
            Mapper<Text, DoubleWritable, IntWritable, Text>{
        private IntWritable pageId = new IntWritable();
        private Text wordTFIDF = new Text();
        private Set<Integer> pageIds = new HashSet<Integer>();

        protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException{
            String[] wordPageId = key.toString().split(Tool.SEPARATOR);
            pageId.set(Integer.parseInt(wordPageId[1]));
            pageIds.add(Integer.parseInt(wordPageId[1]));
            wordTFIDF.set(wordPageId[0] + Tool.SEPARATOR + value.toString());
            context.write(pageId, wordTFIDF);
        }

        protected void cleanup(Context context)
            throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            //保存所有网页ID
            Path centerPath = new Path(conf.get("CENTERPATH"));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(centerPath, true);
            SequenceFile.Writer out
                    = SequenceFile.createWriter(fs, conf, centerPath, IntWritable.class, IntWritable.class);

            IntWritable pageId = new IntWritable();
            for(int id : pageIds){
                pageId.set(id);
                out.append(pageId, pageId);
            }
            //System.out.println("Build" + " " + pageIds);
            out.close();
            super.cleanup(context);
        }
    }

    public static class BuildVectorReducer extends
            Reducer<IntWritable, Text, IntWritable, Text> {
        private Map<String, Integer> wordDict = new HashMap<String, Integer>();
        private Text wordIdTFIDF = new Text();
        private List<Integer> center = new LinkedList<Integer>();
        private Map<Integer, String> centerTFIDF = new HashMap<Integer, String>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            //读取词表
            wordDict = Tool.readWordDict(conf, conf.get("DICTPATH"));

            //读取所有网页ID
            List<Integer> pageIds = new LinkedList<Integer>();
            Path centerPath = new Path(conf.get("CENTERPATH"));
            SequenceFile.Reader centerReader = new SequenceFile.Reader(fs, centerPath, conf);
            IntWritable temp = new IntWritable();
            while (centerReader.next(temp)) {
                pageIds.add(temp.get());
            }
            centerReader.close();


            //选取K个簇心
            int k = conf.getInt("K", 1);
            Collections.shuffle(pageIds);
            for (int i = 0; i < k; i++) {
                center.add(pageIds.get(i));
            }


            super.setup(context);
        }

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (key.get() == -1)
                System.out.println("FUCKKKKKKKKKKKKKKKKKKKKKKK");
            StringBuilder allWordTFIDFTemp = new StringBuilder();
            for (Text value : values) {
                String[] wordTFIDFs = value.toString().split(Tool.SEPARATOR);
                String word = wordTFIDFs[0];
                String tfidf = wordTFIDFs[1];
                allWordTFIDFTemp.append(wordDict.get(word));
                allWordTFIDFTemp.append(Tool.SEPARATOR);
                allWordTFIDFTemp.append(tfidf);
                allWordTFIDFTemp.append(Tool.AND);

            }

            if (center.contains(key.get()))
                centerTFIDF.put(key.get(), allWordTFIDFTemp.toString());

            wordIdTFIDF.set(allWordTFIDFTemp.toString());
            context.write(key, wordIdTFIDF);
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            //保存中心
            Path centerPath = new Path(conf.get("CENTERPATH"));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(centerPath, true);
            SequenceFile.Writer out
                    = SequenceFile.createWriter(fs, conf, centerPath, IntWritable.class, Text.class);

            int i = 0;
            for (int index : centerTFIDF.keySet()) {
                //System.out.println(centerTFIDF.get(index));
                out.append(new IntWritable(i), new Text(centerTFIDF.get(index)));
                i++;
            }
            //System.out.println(i);
            out.close();
            super.cleanup(context);
        }
    }
    public static void main(String[] args) throws Exception{

        //buildVectorInput, buildVectorOutput, DICTPATH, CENTERPATH, K
        Configuration conf = new Configuration();
        conf.set("DICTPATH", args[2]);
        conf.set("CENTERPATH", args[3]);
        conf.set("K", args[4]);

        Job job = Job.getInstance(conf, "buildVector");

        job.setJarByClass(BuildVector.class);
        job.setMapperClass(BuildVectorMapper.class);
        job.setReducerClass(BuildVectorReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path buildVectorOutput = new Path(args[1]);
        fs.delete(buildVectorOutput, true);
        FileOutputFormat.setOutputPath(job, buildVectorOutput);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
