package backend.common;

import common.Error;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;               // 实际缓存的数据
    private HashMap<Long, Integer> references;    // 引用计数
    private HashMap<Long, Boolean> getting;       // 正在获取的资源

    private int maxResource;
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.cache = new HashMap<>();
        this.references = new HashMap<>();
        this.getting = new HashMap<>();

        this.maxResource = maxResource;
        this.lock = new ReentrantLock();
    }

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T object);

    /**
     * 获取资源
     * 1. 从缓存中获取
     * 2. 缓存中没找到, 则从数据源加载并获取
     * 3. 加载失败, 抛出异常
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }

                continue;
            }

            if (cache.containsKey(key)) {
                // 需要获取的资源在缓存中, 直接返回数据
                T object = cache.get(key);
                references.put(key, references.getOrDefault(key, 0) + 1);
                lock.unlock();

                return object;
            }

            // 判断缓存池是否已满
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 没满则将资源添加到缓存池
            count += 1;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T object = null;
        try {
            object = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count -= 1;
            getting.remove(key);
            lock.unlock();
            throw e;                   // 该错误没有被捕获, 程序会异常退出
        }

        lock.lock();
        getting.remove(key);           // 资源获取完成后删除 key
        cache.put(key, object);
        references.put(key, 1);
        lock.unlock();

        return object;
    }

    /**
     * 释放缓存
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T object = cache.get(key);
                releaseForCache(object);

                references.remove(key);
                cache.remove(key);
                count -= 1;
            }
            else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存, 清空缓存池, 回收资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T object = cache.get(key);
                releaseForCache(object);

                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
