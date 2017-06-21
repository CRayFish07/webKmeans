package com.spider;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by wqlin on 17-6-19.
 * 消费者，爬取文章主页
 */
public class WebPageConsumer implements Runnable {
    private String resultDir;


    public WebPageConsumer(String resultDir) {
        this.resultDir = resultDir;
    }

    public void parse() throws IOException, InterruptedException {
        String URL;
        while (!(URL = Container.getURLQueue().take()).equals("end")) {
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
                Utils.writeToFile(resultDir + "raw.txt", articleBody);
                System.out.println("Processing: " + URL + " UID: " + Container.getURLToUIDMap().get(URL));
            } catch (NullPointerException e) {
                System.out.println("Error in processing: " + URL + " UID: " + Container.getURLToUIDMap().get(URL));
                Thread.sleep(3000);
                Container.getURLQueue().put(URL);
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
