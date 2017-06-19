package com.bigdata;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
    final private static String SEPARATOR = "@";
    final private static String AND = "&";

    public static class BuildVectorMapper extends
            Mapper<Text, DoubleWritable, LongWritable, Text>{
        private LongWritable pageId = new LongWritable();
        private Text wordTFIDF = new Text();
        private List<Long> pageIds = new LinkedList<Long>();

        protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException{
            String[] wordPageId = key.toString().split(SEPARATOR);
            pageId.set(Long.parseLong(wordPageId[1]));
            pageIds.add(Long.parseLong(wordPageId[1]));
            wordTFIDF.set(wordPageId[0] + SEPARATOR + value.toString());
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
                    = new SequenceFile.Writer(fs, conf, centerPath, LongWritable.class, LongWritable.class);

            LongWritable pageId = new LongWritable();
            for(long id : pageIds){
                pageId.set(id);
                out.append(pageId, pageId);
            }
            out.close();
            super.cleanup(context);
        }
    }

    public static class BuildVectorReducer extends
            Reducer<LongWritable, Text, LongWritable, Text> {
        private Map<String, Long> wordDict = new HashMap<String, Long>();
        private Text wordIdTFIDF = new Text();
        private List<Long> center = new LinkedList<Long>();
        private Map<Long, String> centerTFIDF = new HashMap<Long, String>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            //读取词表
            Path dictPath = new Path(conf.get("DICTPATH"));
            SequenceFile.Reader dictReader = new SequenceFile.Reader(fs, dictPath, conf);
            Text word = new Text();
            LongWritable index = new LongWritable();
            while (dictReader.next(word, index))
                wordDict.put(word.toString(), index.get());
            dictReader.close();

            //读取所有网页ID
            List<Long> pageIds = new LinkedList<Long>();
            Path centerPath = new Path(conf.get("CENTERPATH"));
            SequenceFile.Reader centerReader = new SequenceFile.Reader(fs, centerPath, conf);
            LongWritable temp = new LongWritable();
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

        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder allWordTFIDFTemp = new StringBuilder();
            for (Text value : values) {
                String[] wordTFIDFs = value.toString().split(SEPARATOR);
                String word = wordTFIDFs[0];
                String tfidf = wordTFIDFs[1];
                allWordTFIDFTemp.append(wordDict.get(word));
                allWordTFIDFTemp.append(SEPARATOR);
                allWordTFIDFTemp.append(tfidf);
                allWordTFIDFTemp.append(AND);

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
                    = new SequenceFile.Writer(fs, conf, centerPath, LongWritable.class, Text.class);

            int i = 0;
            for (long index : centerTFIDF.keySet()) {
                out.append(new LongWritable(i), new Text(centerTFIDF.get(index)));
                i++;
            }
            out.close();
            super.cleanup(context);
        }
    }
    public static void main(String[] args) throws Exception{

        //buildVectorInput, buildVectorOutput, DICTPATH, CENTERPATH
        Configuration conf = new Configuration();
        conf.set("DICTPATH", args[2]);
        conf.set("CENTERPATH", args[3]);

        Job job = Job.getInstance(conf, "buildVector");

        job.setJarByClass(BuildVector.class);
        job.setMapperClass(BuildVectorMapper.class);
        job.setReducerClass(BuildVectorReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path buildVectorOutput = new Path(args[1]);
        fs.delete(buildVectorOutput, true);
        FileOutputFormat.setOutputPath(job, buildVectorOutput);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
