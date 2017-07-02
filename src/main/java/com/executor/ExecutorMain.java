package com.executor;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created by wqlin on 17-6-19.
 * 主程序入口
 */
public class ExecutorMain {

    public static void main(String[] args) {
        final int CLUSTER_SIZE = 100;
        final int CONSUMER_THREAD_SIZE = 4;
        final String resultDirPath = "./input/";
        File resultDir = new File(resultDirPath);
        try {
            FileUtils.deleteDirectory(resultDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!resultDir.exists())
            resultDir.mkdir();


        ArrayBlockingQueue<String> URLQueue = new ArrayBlockingQueue<String>(500);
        ConcurrentHashMap<String, Integer> URLToUIDMap = new ConcurrentHashMap<String, Integer>();
        ConcurrentHashMap<Integer, Integer> UIDToClusterMap = new ConcurrentHashMap<Integer, Integer>();

        ExecutorService producerExecutor = Executors.newSingleThreadExecutor();
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(CONSUMER_THREAD_SIZE);
        producerExecutor.execute(
                new WebPageProducer(
                        URLQueue,
                        URLToUIDMap,
                        UIDToClusterMap,
                        CLUSTER_SIZE));
        for (int i = 0; i < CONSUMER_THREAD_SIZE; i++)
            consumerExecutor.execute(
                    new WebPageConsumer(
                            URLQueue,
                            URLToUIDMap,
                            UIDToClusterMap,
                            resultDirPath));

        producerExecutor.shutdown();
        consumerExecutor.shutdown();

        try {
            producerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            consumerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//        Thread producer = new Thread(
//                new WebPageProducer(
//                        URLQueue,
//                        URLToUIDMap,
//                        UIDToClusterMap,
//                        CLUSTER_SIZE));
//
//        Thread consumer = new Thread(
//                new WebPageConsumer(
//                        URLQueue,
//                        URLToUIDMap,
//                        UIDToClusterMap,
//                        resultDirPath));
//
//        consumer.start();
//        producer.start();
//
//        try {
//            producer.join();
//            consumer.join();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
