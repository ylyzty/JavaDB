package backend.dm.page;

/**
 * 页定义
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNo();
    byte[] getData();
}
