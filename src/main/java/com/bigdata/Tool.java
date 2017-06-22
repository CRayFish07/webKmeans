package com.bigdata;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by ivan on 6/21/17.
 */

public class Tool {

    final public static String SEPARATOR = "@";
    final public static String AND = "&";

    /**
     * 读取词表
     * @param conf
     * @param path
     * @return
     * @throws IOException
     */
    public static Map<String, Integer> readWordDict(Configuration conf, String path)
        throws IOException{

        Map<String, Integer> wordDict = new HashMap<String, Integer>();

        FileSystem fs = FileSystem.get(conf);
        Path dictPath = new Path(path);
        SequenceFile.Reader dictReader = new SequenceFile.Reader(fs, dictPath, conf);
        Text word = new Text();
        IntWritable index = new IntWritable();
        while (dictReader.next(word, index))
            wordDict.put(word.toString(), index.get());
        dictReader.close();

        return wordDict;
    }

    /**
     * 读取簇心文件
     * @param conf
     * @param path
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static Map<Integer, Map<Integer, Double>> readCenter(Configuration conf, String path)
        throws IOException, InterruptedException{
        Map<Integer, Map<Integer, Double>> centers = new HashMap<Integer, Map<Integer, Double>>();

        Path centerPath = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader centerReader = new SequenceFile.Reader(fs, centerPath, conf);
        IntWritable centerId = new IntWritable();
        Text tfidfs = new Text();
        while (centerReader.next(centerId, tfidfs)){
            Map<Integer, Double> wordTFIDF = Tool.text2map(tfidfs);
            centers.put(centerId.get(), wordTFIDF);
        }
        centerReader.close();
        return centers;
    }


    /**
     * 将tfidfs转为Map<Integer, Double>
     * @param tfidfs 单词编号1|TFIDF值1&单词编号2|TFIDF值2...
     * @return
     */
    public static Map<Integer, Double> text2map(Text tfidfs){
        Map<Integer, Double> wordTfidf = new HashMap<Integer, Double>();
        String[] wts = tfidfs.toString().split(AND);
        for (String wt :wts){
            wordTfidf.put(Integer.parseInt(wt.split(SEPARATOR)[0]),
                    Double.parseDouble(wt.split(SEPARATOR)[1]));
        }
        return wordTfidf;
    }

    /**
     * 找出与改样本距离最近的中心
     * @param page
     * @param centers
     * @param wordDictSisze
     * @return
     */
    public static int getNearestNeighbour(Map<Integer, Double> page, Map<Integer, Map<Integer, Double>> centers, int wordDictSisze){
        int index = -1;
        double miniDist = Double.MIN_VALUE;
        Random random = new Random();
        int rand = random.nextInt();
        //System.out.println(rand + "page:" + page.toString());
        for(int i : centers.keySet()){
            double dist = getDistance(centers.get(i), page, wordDictSisze);
            if (dist > miniDist){
                miniDist = dist;
                index = i;
            }
        }
        //System.out.println(rand + "center:" + centers.get(index).toString());
        //System.out.println(rand + " " + getDistance(centers.get(1), page, wordDictSisze));
        //System.out.println(rand + " " + getDistance(centers.get(0), page, wordDictSisze));
        return index;
    }


    /**
     * 计算该网页与中心的余弦距离
     * @param center
     * @param page
     * @param wordDictSize
     * @return
     */
    public static double getDistance(Map<Integer, Double> center, Map<Integer, Double> page, int wordDictSize){
        double sum = 0;
        double sum1 = 0;
        double sum2 = 0;
        for(int i = 0; i < wordDictSize; i++){
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
