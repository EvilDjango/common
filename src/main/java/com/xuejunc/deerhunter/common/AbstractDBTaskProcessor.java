package com.xuejunc.deerhunter.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 数据库任务处理抽象类：一个生产者向数据库轮询任务，多个消费者处理任务
 * <p>
 * Created on
 * Copyright (c) 2019, deerhunter0837@gmail.com All Rights Reserved.
 *
 * @author xuejunc
 * @date 2019-05-21
 */
public abstract class AbstractDBTaskProcessor<E> implements Runnable {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    /**
     * 工程实例部署数量
     */
    private static final int N_DEPLOYMENT = 2;
    /**
     * 生产者空闲时休眠的秒数
     */
    private static long sleepSecondsOnIdle = 3;
    /**
     * 数据库记录队列
     */
    protected BlockingQueue<E> recordQueue;

    /**
     * 重试队列
     */
    protected BlockingQueue<E> retryQueue;
    /**
     * 数据库记录队列的容量
     */
    protected int queueSize;

    private ExecutorService executor;
    /**
     * 重试线程池
     */
    private ScheduledExecutorService retryPool;
    /**
     * 消费者线程数
     */
    private int nConsumer;
    /**
     * 单个工作线程内调用服务商接口的间隔纳秒数
     */
    private long requestIntervalNanosInSingleThread;


    /**
     * 构造函数
     *
     * @param nConsumer     工作线程数
     * @param queueSize     队列容量
     * @param qpsLimit      qps限制，单位：请求次数/分钟，qpsLimit<=0 时，认为无限制。
     * @param openRetryPool 开启重试池
     */
    public AbstractDBTaskProcessor(int nConsumer, int queueSize, int qpsLimit, String threadNamePrefix, boolean openRetryPool) {

        recordQueue = new ArrayBlockingQueue<>(queueSize);
        this.queueSize = queueSize;
        int nThread = openRetryPool ? nConsumer + 2 : nConsumer + 1;
        NamedThreadFactory threadFactory = new NamedThreadFactory(threadNamePrefix);
        executor = new ThreadPoolExecutor(nThread, nThread, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                threadFactory);
        if (openRetryPool) {
            retryPool = Executors.newSingleThreadScheduledExecutor(threadFactory);
            retryQueue = new ArrayBlockingQueue<>(1000);
        }
        this.nConsumer = nConsumer;
        requestIntervalNanosInSingleThread = getRequestIntervalNanosInSingleConsumer((nConsumer) * N_DEPLOYMENT, qpsLimit);
    }

    public AbstractDBTaskProcessor(int nConsumer, int queueSize, int qpsLimit, String threadNamePrefix) {
        this(nConsumer, queueSize, qpsLimit, threadNamePrefix, false);
    }

    /**
     * 获取单个工作线程内每次调用服务商接口的间隔纳秒数
     * 如果qpsLimit<=0,认为qps无限制
     *
     * @param nThread  调用服务商接口的线程数
     * @param qpsLimit qps限制：次数/分钟
     * @return
     */
    private static long getRequestIntervalNanosInSingleConsumer(int nThread, int qpsLimit) {
        if (qpsLimit <= 0) {
            return 0;
        }
        float qpsLimitPerThread = (float) qpsLimit / nThread;
        return (long) (TimeUnit.MINUTES.toNanos(1) / qpsLimitPerThread);
    }

    /**
     * 将所有队列中的任务的状态恢复到入队列前
     */
    private void recoverTaskStateInQueue() {
        for (E e : recordQueue) {
            recoverOneTask(e);
        }
        if (retryQueue != null) {
            for (E e : retryQueue) {
                recoverOneTask(e);
            }
        }
    }

    /**
     * 将单个任务的状态恢复到入队列前
     *
     * @param task
     */
    protected abstract void recoverOneTask(E task);

    /**
     * 将数据库中所有状态为队列中的任务状态恢复为入队列前的状态
     */
    public abstract void recoverTaskState();

    /**
     * 启动工作线程
     */
    @Override
    public void run() {
        recoverTaskState();
        Runtime.getRuntime().addShutdownHook(new Thread(this::recoverTaskStateInQueue));
        executor.execute(new Producer());
        for (int i = 0; i < nConsumer; i++) {
            executor.execute(new Consumer());
        }
    }

    /**
     * 获取相关的数据库记录
     *
     * @return
     */
    protected abstract List<E> query();

    /**
     * 对数据库记录进行处理
     *
     * @param e
     */
    protected abstract void process(E e);

    /**
     * 是否可用，不可用时消费者将不处理任务
     *
     * @return
     */
    protected boolean isAvailable() {
        return true;
    }

    /**
     * 新增延迟重试任务
     *
     * @param e             重试任务要处理的元素
     * @param delayedMillis 延迟毫秒数
     * @return
     */
    protected boolean addDelayedRetryTask(E e, long delayedMillis) {
        if (null == retryPool) {
            throw new IllegalArgumentException("Retry pool is not open");
        }
        boolean inRetryQue = retryQueue.offer(e);

        // 入重试队列失败，返回失败
        if (!inRetryQue) {
            return false;
        }

        // 入重试队列成功，新建一个延时任务，一定的时间后将任务再次加入工作队列
        retryPool.schedule(() -> {
            try {
                //从重试队列移除
                if (retryQueue.remove(e)) {
                    // 将重试任务重新放入工作队列
                    recordQueue.put(e);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, delayedMillis, TimeUnit.MILLISECONDS);

        return true;
    }

    /**
     * 生产者，持续从数据库查询相关记录放到队列{@code recordQueue}中
     */
    protected class Producer implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                List<E> records = query();
                if (0 == records.size()) {
                    logger.debug("Producer sleep on idle.");
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(sleepSecondsOnIdle));
                }

                for (E e : records) {
                    try {
                        // 当队列已满时阻塞
                        recordQueue.put(e);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * 消费者，持续从队列{@code recordQueue}中取数据库记录，进行相应处理
     */
    protected class Consumer implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                if (!isAvailable()) {
                    logger.debug("Consumer sleep on idle when service is not available");
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(sleepSecondsOnIdle));
                    continue;
                }
                long start = System.nanoTime();
                try {
                    // 当队列为空时阻塞
                    E e;
                    while (true) {
                        e = recordQueue.take();
                        if (!isUpdatedByOthers(e)) {
                            break;
                        }

                        // 如果该记录已经被别的线程更新过了，跳过
                        logger.warn("Repeated processing, going to ignore. Task: {}", e);
                    }

                    process(e);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    long duration = System.nanoTime() - start;
                    // 休眠，防止超出qps限制
                    LockSupport.parkNanos(requestIntervalNanosInSingleThread - duration);
                }
            }
        }
    }

    /**
     * 数据库记录是否已经被别的线程更新
     *
     * @param e
     * @return
     */
    protected abstract boolean isUpdatedByOthers(E e);
}