package com.lacus.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class JobExecuteThreadPoolUtil {

    private static final int corePoolSize = 10;

    private static final int maximumPoolSize = 100;

    private static final long keepAliveTime = 10;

    private static ThreadPoolExecutor threadPoolExecutor;

    private static JobExecuteThreadPoolUtil asyncThreadPool;

    private JobExecuteThreadPoolUtil() {
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(200, true);
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.MINUTES, workQueue);
    }

    public static synchronized JobExecuteThreadPoolUtil getInstance() {
        if (null == asyncThreadPool) {
            synchronized (JobExecuteThreadPoolUtil.class) {
                if (null == asyncThreadPool) {
                    asyncThreadPool = new JobExecuteThreadPoolUtil();
                }
            }
        }
        log.info("JobThreadPool threadPoolExecutor={}", threadPoolExecutor);
        return asyncThreadPool;
    }

    public synchronized ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

}
