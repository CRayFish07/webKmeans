package com.spider;

import java.io.PrintWriter;

/**
 * Created by wqlin on 17-6-20.
 * 工具类
 */
public class Utils {
    public static void writeToFile(String fileName, String content) {
        try {
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            writer.println(content);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
