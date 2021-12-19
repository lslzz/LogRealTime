package com.cetecloud.logalarm.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPollUtil {
    private static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance() {
        if (pool == null) {
            synchronized (ThreadPollUtil.class) {
                if (pool == null) {
                    pool = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>()
                    );
                }
            }
        }
        return pool;
    }
}
