package com.spider;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wqlin on 17-6-19.
 * 消费者，爬取文章主页
 */
public class WebPageConsumer implements Runnable {
    private static ArrayBlockingQueue<String> URLQueue;
    private static ConcurrentHashMap<String, Integer> URLMap;
    private String resultDir;

    public WebPageConsumer(ArrayBlockingQueue<String> URLQueue,
                           ConcurrentHashMap<String ,Integer> URLMap,
                           String resultDir) {
        URLQueue = URLQueue;
        URLMap=URLMap;
        this.resultDir = resultDir;
    }

    public void parse() throws IOException, InterruptedException {
        String URL;
        while (!(URL = URLQueue.poll()).equals("end")) {
            Document document = Jsoup.connect(URL).get();
            String articleBody = document.getElementById("article").toString();
            Utils.writeToFile(resultDir + URLMap.get(URL), articleBody);
        }


    }

    public void run() {
        try {
            parse();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
