package com.spider;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;

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
            Document document = Jsoup.connect(URL).get();
            //if(document.body().toString());
            try {
                String articleBody = document.getElementById("article").toString();
                Utils.writeToFile(resultDir + Container.getURLToUIDMap().get(URL), articleBody);
            } catch (NullPointerException e) {
                e.printStackTrace();
                System.out.print(URL);
            }
        }
        PrintWriter writer = new PrintWriter("UIDToCluster", "UTF-8");
        for (ConcurrentHashMap.Entry<Integer, Integer> e : Container.getUIDToClusterMap().entrySet()) {
            writer.println(e.getKey() + " " + e.getValue());
        }
        writer.close();
    }

    public void run() {
        try {
            parse();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
