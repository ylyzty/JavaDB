package backend.tm;

import common.Error;
import utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TM: xid 文件维护事务状态, 提供接口供其他模块创建以及查询事务的状态
 */
public interface TransactionManager {
    // 变量默认类型: public static final
    // XID 文件头的长度(Byte)
    static final int XID_HEADER_LENGTH = 8;
    // 记录每个事务状态占用的空间(Byte)
    static final int XID_SIZE = 1;
    static final String XID_FILE_SUFFIX = ".xid";

    // 方法默认访问权限: public
    long begin();             // 开启事务
    void commit(long xid);    // 提交事务
    void abort(long xid);     // 回滚事务
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    /**
     * 创建新的 xid 文件, 并创建 TransactionManager
     * @param path
     * @return
     */
    static TransactionManagerImpl create(String path) {
        File f = new File(path + XID_FILE_SUFFIX);

        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 确定文件可读写
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

    /**
     * 从已有 xid 文件创建 TransactionManager
     * @param path
     * @return
     */
    static TransactionManagerImpl open(String path) {
        File f = new File(path + XID_FILE_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        // 确定文件可读写
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }
}
