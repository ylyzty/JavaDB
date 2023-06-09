package backend.dm.page;

import backend.dm.PageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{

    private int pageNo;                  // 页号, 从 1 开始
    private byte[] data;                 // 页包含的数据
    private boolean dirty;               // 是否为脏页, 脏页需写回磁盘
    private PageCache pageCache;         // 缓存池
    private Lock lock;

    public PageImpl(int pageNo, byte[] data, PageCache pageCache) {
        this.pageNo = pageNo;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
