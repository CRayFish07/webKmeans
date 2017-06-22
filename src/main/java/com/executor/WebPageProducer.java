package com.executor;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by wqlin on 17-6-19.
 * 生产者,产生文章链接
 */
public class WebPageProducer implements Runnable {
    private static String[] startURLs = {
            "https://www.myzaker.com/channel/9",//娱乐频道
            "https://www.myzaker.com/channel/7",//汽车频道
            "https://www.myzaker.com/channel/8",//体育频道
            "https://www.myzaker.com/channel/13",//科技频道
            "https://www.myzaker.com/channel/3",//军事频道
            "https://www.myzaker.com/channel/10386",//美食频道
            "https://www.myzaker.com/channel/12",//时尚频道
            "https://www.myzaker.com/channel/981",//旅游频道
            "https://www.myzaker.com/channel/10376",//游戏频道
            "https://www.myzaker.com/channel/10530"//电影频道
    };
    private int clusterSize;
    private volatile int URLUID = 1;


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

    public WebPageProducer(
            ArrayBlockingQueue<String> URLQueue,
            ConcurrentHashMap<String, Integer> URLToUIDMap,
            ConcurrentHashMap<Integer, Integer> UIDToClusterMap,
            int clusterSize) {
        this.URLQueue = URLQueue;
        this.URLToUIDMap = URLToUIDMap;
        this.UIDToClusterMap = UIDToClusterMap;
        this.clusterSize = clusterSize;
    }

    public WebPageProducer() {

    }

    private void extractURL(String URL, int i) throws IOException, InterruptedException {
        int count = 0;
        String nextPage = URL;
        while (true) {
            Document document = Jsoup.connect(nextPage)
                    .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36")
                    .get();
            for (Element element : document.getElementsByAttributeValue("class", "figure flex-block")) {
                String articleURL = element.select("div > h2 > a").first().absUrl("href");
                //synchronized (WebPageProducer.class) {
                if (!URLToUIDMap.containsKey(articleURL)) {
                    URLQueue.put(articleURL);
                    URLToUIDMap.put(articleURL, URLUID);
                    UIDToClusterMap.put(URLUID, i);
                    URLUID += 1;
                    count += 1;
                }
                //}
                if (count >= clusterSize)
                    return;
            }
            nextPage = document.getElementsByAttributeValue("class", "next_page").first().absUrl("href");
            if (nextPage == null || nextPage.equals(URL))
                return;
        }
    }

    public void run() {
        try {
            for (int i = 0; i < startURLs.length; i++)
                extractURL(startURLs[i], i + 1);
            PrintWriter UIDToClusterWriter = new PrintWriter("UIDToCluster.txt", "UTF-8");
            for (ConcurrentHashMap.Entry<Integer, Integer> e : UIDToClusterMap.entrySet()) {
                UIDToClusterWriter.println(e.getKey() + " " + e.getValue());
            }
            UIDToClusterWriter.close();
            PrintWriter URLToUIDWriter = new PrintWriter("URLToUID.txt", "UTF-8");
            for (ConcurrentHashMap.Entry<String, Integer> e : URLToUIDMap.entrySet()) {
                URLToUIDWriter.println(e.getKey() + " " + e.getValue());
            }
            URLToUIDWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
