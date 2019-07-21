package com.home.study.elasticsearch.common;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class AutocloseableStripedLock implements AutoCloseable{
    private Lock lock;

    AutocloseableStripedLock(ReadWriteLock readWriteLock) throws InterruptedException {
        lock = readWriteLock.writeLock();
        lock.tryLock(60, TimeUnit.SECONDS);
    }
    @Override
    public void close() throws Exception {
        lock.unlock();
    }

    public static AutocloseableStripedLock getLock(Striped<ReadWriteLock> createIndexLocks, String indexName) throws InterruptedException {
        return new AutocloseableStripedLock(createIndexLocks.get(indexName));
    }
}
