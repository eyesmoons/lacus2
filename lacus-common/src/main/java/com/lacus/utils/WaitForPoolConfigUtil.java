package com.lacus.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class WaitForPoolConfigUtil {

  private static final int corePoolSize = 20;

  private static final int maximumPoolSize = 400;

  private static final long keepAliveTime = 10;

  private static ThreadPoolExecutor threadPoolExecutor;

  private static WaitForPoolConfigUtil alarmPoolConfig;

  private WaitForPoolConfigUtil() {
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(100, true);
    threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
        TimeUnit.MINUTES,
        workQueue, new ThreadPoolExecutor.AbortPolicy());
  }

  public static synchronized WaitForPoolConfigUtil getInstance() {
    if (null == alarmPoolConfig) {
      synchronized (WaitForPoolConfigUtil.class) {
        if (null == alarmPoolConfig) {
          alarmPoolConfig = new WaitForPoolConfigUtil();
        }
      }
    }
    log.info("WaitForPoolConfig threadPoolExecutor={}", threadPoolExecutor);
    return alarmPoolConfig;
  }

  public synchronized ThreadPoolExecutor getThreadPoolExecutor() {
    return threadPoolExecutor;
  }
}
