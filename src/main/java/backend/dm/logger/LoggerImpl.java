package backend.dm.logger;

import com.google.common.primitives.Bytes;
import common.Error;
import utils.Panic;
import utils.Parser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/***
 * 操作日志文件
 *
 * 日志文件格式:  [XChecksum] [log1] [log2] ... [logN] [bad_tail]
 *     - XChecksum ==> 所有日志的总校验和
 *     - bad_tail  ==> 数据库崩溃时，没有来得及写完的日志数据
 * 日志格式: [size] [checksum] [data]
 *     - size: 4 byte, int  ==> data length
 *     - checksum: 4 byte, int  ==> 单条日志的校验和
 */
public class LoggerImpl implements Logger{
    private static final int SEED = 13331;

    // log
    public static final int SIZE_OFFSET = 0;
    public static final int CHECKSUM_OFFSET = SIZE_OFFSET + 4;
    public static final int DATA_OFFSET = CHECKSUM_OFFSET + 4;

    public static final String LOG_FILE_SUFFIX = ".log";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private long position;    // 当前日志指针的位置
    private long fileSize;    // 日志文件大小, 初始化时确定
    private int xChecksum;

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.raf = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        this.lock = new ReentrantLock();
    }

    /**
     * 日志文件初始化
     */
    public void init() {
        long size = 0;
        try {
            size = this.raf.length();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (Exception e) {
            Panic.panic(e);
        }

        int xChecksum = Parser.parseBytesToInt(buf.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 写入日志到文件
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);    // 字节数组 ==> 缓冲区

        lock.lock();
        try {
            fc.position(fc.size());    // 定位到文件末尾
            fc.write(buf);

            updateXChecksum(log);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 组装日志  [data] ==> [size, checksum, data]
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.parseIntToBytes(computeChecksum(0, data));
        byte[] size = Parser.parseIntToBytes(data.length);

        return Bytes.concat(size, checksum, data);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = computeChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.parseIntToBytes(xChecksum)));
            fc.force(false);    // 强制将 FileChannel 中的数据写入磁盘
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            // 截断
            this.fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 迭代器模式: 通过该方法不断从文件中读取下一条日志
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }

            return Arrays.copyOfRange(log, DATA_OFFSET, log.length);
        } finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        // 指针已经到文件尾部, 或者到最后一条未写完的日志
        if (position + DATA_OFFSET >= fileSize) {
            return null;
        }

        // 申请一个 4 字节的缓冲区
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size = Parser.parseBytesToInt(tmp.array());
        if (position + size + DATA_OFFSET > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(DATA_OFFSET + size);    // data length + 8 bytes
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checksumCurrent = computeChecksum(0, Arrays.copyOfRange(log, DATA_OFFSET, log.length));
        int checksumOrigin = Parser.parseBytesToInt(Arrays.copyOfRange(log, CHECKSUM_OFFSET, DATA_OFFSET));
        if (checksumCurrent != checksumOrigin) {
            return null;
        }

        position += log.length;
        return log;
    }

    /**
     * 回到日志数据开始位置
     */
    @Override
    public void rewind() {
        this.position = 4;
    }

    /**
     * 校验日志文件的 XChecksum, 并移除尾部的 Bad tail
     */
    private void checkAndRemoveTail() {
        rewind();

        // 循环计算校验和
        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }

            xCheck = computeChecksum(xCheck, log);
        }

        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 截断文件到正常位置的末尾
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            raf.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        rewind();
    }

    /**
     * 计算单挑日志的 checksum
     * @param xCheck
     * @param log
     * @return
     */
    private int computeChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            // 计算会溢出, 但可以正常校验
            xCheck = xCheck * SEED + b;
        }

        return xCheck;
    }

    @Override
    public void close() {
        try {
            this.fc.close();
            this.raf.close();
        } catch (Exception e) {
            Panic.panic(e);
        }
    }
}
