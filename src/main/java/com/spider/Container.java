package com.spider;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wqlin on 17-6-20.
 */
public class Container {
    private static ArrayBlockingQueue<String> URLQueue;
    private static ConcurrentHashMap<String, Integer> URLToUIDMap;
    private static ConcurrentHashMap<Integer, Integer> UIDToClusterMap;

    public static void setUIDToClusterMap(ConcurrentHashMap<Integer, Integer> uidToClusterMap) {
        UIDToClusterMap = uidToClusterMap;
    }

    public static ConcurrentHashMap<Integer, Integer> getUIDToClusterMap() {
        return UIDToClusterMap;
    }

    public static void setURLQueue(ArrayBlockingQueue<String> urlQueue) {
        URLQueue = urlQueue;
    }

    public static ArrayBlockingQueue<String> getURLQueue() {
        return URLQueue;
    }

    public static void setURLToUIDMap(ConcurrentHashMap<String, Integer> urlMap) {
        URLToUIDMap = urlMap;
    }

    public static ConcurrentHashMap<String, Integer> getURLToUIDMap() {
        return URLToUIDMap;
    }
}
