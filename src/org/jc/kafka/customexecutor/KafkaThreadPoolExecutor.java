/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kafka.customexecutor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Jorge Céspedes
 * Custom ThreadPoolExecutor to log exceptions that may raise inside any producing
 * or consuming thread.
 */
public class KafkaThreadPoolExecutor extends ThreadPoolExecutor {

    public KafkaThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }
    
    public KafkaThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future<?>) {
        try {
            Future<?> future = (Future<?>) r;
            if (future.isDone()) {
                future.get();
            }
        } catch (CancellationException | ExecutionException ex) {
            t = ex;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt(); // ignore/reset
        }
      }
      if (t != null) {
            System.out.println(t);
      }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return super.submit(task); //To change body of generated methods, choose Tools | Templates.
    }
}
