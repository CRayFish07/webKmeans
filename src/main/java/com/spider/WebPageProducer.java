package com.spider;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.PrintWriter;
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

    public WebPageProducer(
            int clusterSize) {
        this.clusterSize = clusterSize;
    }

    private void extractURL(String URL, int i) throws IOException, InterruptedException {
        int count = 0;
        String nextPage = URL;
        while (true) {
            Document document = Jsoup.connect(nextPage).get();
            for (Element element : document.getElementsByAttributeValue("class", "figure flex-block")) {
                String articleURL = element.select("div > h2 > a").first().absUrl("href");
                synchronized (WebPageProducer.class) {
                    if (!Container.getURLToUIDMap().containsKey(articleURL)) {
                        Container.getURLQueue().put(articleURL);
                        Container.getURLToUIDMap().put(articleURL, URLUID);
                        Container.getUIDToClusterMap().put(URLUID, i);
                        URLUID += 1;
                        count += 1;
                    }
                }
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
            for (ConcurrentHashMap.Entry<Integer, Integer> e : Container.getUIDToClusterMap().entrySet()) {
                UIDToClusterWriter.println(e.getKey() + " " + e.getValue());
            }
            UIDToClusterWriter.close();
            PrintWriter URLToUIDWriter = new PrintWriter("URLToUID.txt", "UTF-8");
            for (ConcurrentHashMap.Entry<String, Integer> e : Container.getURLToUIDMap().entrySet()) {
                URLToUIDWriter.println(e.getKey() + " " + e.getValue());
            }
            URLToUIDWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
