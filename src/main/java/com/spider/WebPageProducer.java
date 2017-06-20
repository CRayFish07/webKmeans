package com.spider;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wqlin on 17-6-19.
 * 生产者
 */
public class WebPageProducer extends Thread {
    private static String[] startURLs = {
            "https://www.myzaker.com/channel/9",//娱乐频道
            "https://www.myzaker.com/channel/7",//汽车频道
            "https://www.myzaker.com/channel/8",//体育频道
            "https://www.myzaker.com/channel/13",//科技频道
            "https://www.myzaker.com/channel/3",//军事频道
            "https://www.myzaker.com/channel/10386",//美食频道
            "https://www.myzaker.com/channel/12",//时尚频道
            "https://www.myzaker.com/channel/981",//旅游频道
            "https://www.myzaker.com/channel/1037",//游戏频道
            "https://www.myzaker.com/channel/10530"//电影频道
    };

    private final int assignedURLID;

    private static AtomicInteger urlCount = new AtomicInteger(0);


    public WebPageProducer(int id) {
        this.assignedURLID = id;
    }

    private String findChromeDriver(String path) {
        ClassLoader classLoader = getClass().getClassLoader();
        return classLoader.getResource(path).getFile();
    }

    public synchronized WebDriver scrollToBottom(WebDriver driver, int time) throws InterruptedException {
        String oldpage = "";
        String newpage = "";
        do {
            oldpage = driver.getPageSource();
            ((JavascriptExecutor) driver)
                    .executeScript("window.scrollTo(0, (document.body.scrollHeight))");
            this.wait(time);
            newpage = driver.getPageSource();
        } while (!oldpage.equals(newpage));
        return driver;
    }

    private void extractURL(WebDriver driver) {
        List<WebElement> webElementList = driver.findElements(By.xpath("//*[@id=\"section\"]/div/div"));
        for (WebElement webElement : webElementList) {
            String url = webElement.findElement(By.xpath("./h2/a")).getAttribute("href");
            urlCount.incrementAndGet();
            System.out.println(url);
        }
    }

    public void parse(String url) {
        DesiredCapabilities capabilities = DesiredCapabilities.phantomjs();
        capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, findChromeDriver("phantomjs/bin/phantomjs"));

        WebDriver driver = new PhantomJSDriver(capabilities);
        driver.get(url);
        try {
            for (int i = 0; i < 10; i++) {
                driver = scrollToBottom(driver, 5000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.extractURL(driver);


        driver.quit();
    }

    @Override
    public void run() {
        parse(startURLs[assignedURLID]);
    }

//    public static void main(String[] args) {
////        WebPageProducer demo = new WebPageProducer(1);
////        demo.parse(startURLs[1]);
//
//
//        ArrayList<WebPageProducer> seleniumDemoList = new ArrayList<WebPageProducer>();
//        for (int i = 0; i < startURLs.length; i++) {
//            WebPageProducer seleniumDemo = new WebPageProducer(i);
//            seleniumDemoList.add(seleniumDemo);
//            seleniumDemo.start();
//        }
//        try {
//            for (int i = 0; i < startURLs.length; i++)
//                seleniumDemoList.get(i).join();
//            System.out.println(urlCount.get());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
