package com.csjamesdu.datafeed.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyRetryTemplate<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyRetryTemplate.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    private final int attempts;
    private final long timeout;

    private int retryCount;

    public MyRetryTemplate(int attempts, long timeout) {
        this.attempts = attempts;
        this.timeout = timeout;
    }

    public T execute(Callable<T> task) throws Exception {
        final int cores = Runtime.getRuntime().availableProcessors();
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(cores);
        executor.setRemoveOnCancelPolicy(true);
        try {
            final Future<T> taskFuture = executor.submit(task);
            executor.schedule(() -> {
                if (taskFuture.cancel(true)) {
                    throw new RuntimeException("Force catch treatment on task cancel.");
                }
                // If the task could not be cancelled, typically because it has already completed normally.
            }, timeout, TimeUnit.MILLISECONDS);
            final T result = taskFuture.get();
            return result;
        } catch (Exception e) {
            if (++retryCount == attempts) {
                final String messageTemplate = "%d attempts to retry failed at %d ms interval.";
                throw new RuntimeException(String.format(messageTemplate, attempts, timeout), e);
            } else {
                LOGGER.info("DB query timeout! Retry " + retryCount + " starts at: " + dateFormat.format(new Date()));
                return execute(task);
            }
        } finally {
            executor.shutdown();
            retryCount = 0;
        }
    }

    public int getRetryCount() {
        return retryCount;
    }
}
