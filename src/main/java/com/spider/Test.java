package com.spider;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Created by wqlin on 17-6-20.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String url = "https://www.myzaker.com/article/5948ba579490cb3717000027/";
        Document document = Jsoup.connect(url).get();
        System.out.println(document.body().toString());
    }
}
