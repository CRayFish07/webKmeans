package com.spider;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


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
    private static ArrayBlockingQueue<String> URLQueue;
    private int clusterSize;
    private volatile int URLUID = 1;
    private static ConcurrentHashMap<String, Integer> URLMap;

    public WebPageProducer(ArrayBlockingQueue<String> URLQueue,
                           ConcurrentHashMap<String, Integer> URLMap,
                           int clusterSize) {
        URLQueue = URLQueue;
        URLMap = URLMap;
        this.clusterSize = clusterSize;
    }

    private void extractURL(String URL) throws IOException {
        int count = 0;
        String nextPage = URL;
        while (true) {
            Document document = Jsoup.connect(nextPage).get();
            for (Element element : document.getElementsByAttributeValue("class", "figure flex-block")) {
                String articleURL = element.select("div > h2 > a").first().absUrl("href");
                synchronized (WebPageProducer.class) {
                    if (!URLMap.containsKey(articleURL)) {
                        URLMap.put(articleURL, URLUID);
                        URLUID += 1;
                    }
                }
                count += 1;
                if (count >= clusterSize)
                    return;
            }
            nextPage = document.getElementsByAttributeValue("class", "next_page").first().absUrl("href");
            System.out.println("next page: " + nextPage);
            if (nextPage == null || nextPage.equals(URL))
                return;

        }
    }

    public void run() {
        try {
            for (String URL : startURLs)
                extractURL(URL);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ArrayBlockingQueue<String> URLQueue = new ArrayBlockingQueue<String>(200);
        ConcurrentHashMap<String, Integer> URLMap = new ConcurrentHashMap<String, Integer>();
        WebPageProducer webPageProducer = new WebPageProducer(URLQueue, URLMap, 100);
        webPageProducer.run();
    }
}
