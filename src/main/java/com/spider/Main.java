package com.spider;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wqlin on 17-6-19.
 * 主程序入口
 */
public class Main {

    public static void main(String[] args) {
        final int CLUSTER_SIZE = 100;
        final String resultDirPath = "./input/";
        File resultDir = new File(resultDirPath);
        if (!resultDir.exists())
            resultDir.mkdir();

        ArrayBlockingQueue<String> URLQueue = new ArrayBlockingQueue<String>(500);
        ConcurrentHashMap<String, Integer> URLToUIDMap = new ConcurrentHashMap<String, Integer>();
        ConcurrentHashMap<Integer, Integer> UIDToClusterMap = new ConcurrentHashMap<Integer, Integer>();
        Container.setURLToUIDMap(URLToUIDMap);
        Container.setURLQueue(URLQueue);
        Container.setUIDToClusterMap(UIDToClusterMap);

        Thread producer = new Thread(new WebPageProducer(CLUSTER_SIZE));

        Thread consumer = new Thread(new WebPageConsumer(resultDirPath));

        consumer.start();
        producer.start();

        try {
            producer.join();
            consumer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
