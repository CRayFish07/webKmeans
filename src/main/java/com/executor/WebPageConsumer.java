package com.executor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by wqlin on 17-6-19.
 * 消费者，爬取文章主页
 */
public class WebPageConsumer implements Runnable {
    private String resultDir;
    private ArrayBlockingQueue<String> URLQueue;
    private ConcurrentHashMap<String, Integer> URLToUIDMap;
    private ConcurrentHashMap<Integer, Integer> UIDToClusterMap;

    public void setUIDToClusterMap(ConcurrentHashMap<Integer, Integer> uidToClusterMap) {
        this.UIDToClusterMap = uidToClusterMap;
    }

    public ConcurrentHashMap<Integer, Integer> getUIDToClusterMap() {
        return UIDToClusterMap;
    }

    public void setURLQueue(ArrayBlockingQueue<String> urlQueue) {
        this.URLQueue = urlQueue;
    }

    public ArrayBlockingQueue<String> getURLQueue() {
        return URLQueue;
    }

    public void setURLToUIDMap(ConcurrentHashMap<String, Integer> urlMap) {
        this.URLToUIDMap = urlMap;
    }

    public ConcurrentHashMap<String, Integer> getURLToUIDMap() {
        return URLToUIDMap;
    }

    public WebPageConsumer(
            ArrayBlockingQueue<String> URLQueue,
            ConcurrentHashMap<String, Integer> URLToUIDMap,
            ConcurrentHashMap<Integer, Integer> UIDToClusterMap,
            String resultDir) {
        this.URLQueue = URLQueue;
        this.URLToUIDMap = URLToUIDMap;
        this.UIDToClusterMap = UIDToClusterMap;
        this.resultDir = resultDir;
    }

    public void parse() throws IOException, InterruptedException {
        String URL;
        while ((URL = URLQueue.take()) != null) {
            Document document = Jsoup.connect(URL)
                    .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36")
                    .referrer("https://www.myzaker.com/")
                    .get();
            try {
                Elements elements = document.select("div#content");
                String articleBody;
                if (elements.size() > 1)
                    articleBody = elements.get(1).outerHtml();
                else
                    articleBody = elements.outerHtml();
                synchronized (WebPageConsumer.class) {
                    //System.out.println(fileName);
                    PrintWriter writer = new PrintWriter(new FileOutputStream(resultDir + "raw.txt", true));
                    writer.println(URLToUIDMap.get(URL) + "@@@@@@@@@@" + articleBody);
                    writer.println("******************** separating line ********************");
                    writer.flush();
                    writer.close();
                }
                System.out.println("Processing: " + URL + " UID: " + URLToUIDMap.get(URL));
            } catch (NullPointerException e) {
                System.out.println("Error in processing: " + URL + " UID: " + URLToUIDMap.get(URL));
                Thread.sleep(3000);
                URLQueue.put(URL);
            }
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
