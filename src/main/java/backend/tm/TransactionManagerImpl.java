package backend.tm;

import common.Error;
import utils.Panic;
import utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{
    private static final byte TRANSACTION_ACTIVE = 0;
    private static final byte TRANSACTION_COMMITTED = 1;
    private static final byte TRANSACTION_ABORTED = 2;

    private static final long SUPER_XID = 0;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private long xidCounter;    // 维护事务数量
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDFile();
    }

    /**
     * 检查 XID 文件是否合法
     * 同时初始化 xidCounter
     */
    private void checkXIDFile() {
        long fileLength = 0;
        try {
            fileLength = randomAccessFile.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }

        if (fileLength < XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLength) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private long getXidPosition(long xid) {
        return XID_HEADER_LENGTH + (xid - 1) * XID_SIZE;
    }

    /**
     * 更新 xid 事务的状态: status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            // 强制同步缓存到文件
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    /**
     * XID 自增, 更新 file header
     */
    private void increaseXIDCounter() {
        this.xidCounter += 1;
        ByteBuffer buf = ByteBuffer.wrap(Parser.parseLongToByte(xidCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开启一个新事务, 多线程安全
     * @return
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, TRANSACTION_ACTIVE);
            increaseXIDCounter();
            return xid;
        }
        finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, TRANSACTION_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, TRANSACTION_ABORTED);
    }

    /**
     * 事务状态检验函数
     *
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_SIZE]);

        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }

        return checkXID(xid, TRANSACTION_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return true;
        }

        return checkXID(xid, TRANSACTION_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }

        return checkXID(xid, TRANSACTION_ABORTED);
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
