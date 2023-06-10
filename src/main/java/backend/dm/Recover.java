package backend.dm;

import backend.common.SubArray;
import backend.dm.PageCache.PageCache;
import backend.dm.dataItem.DataItem;
import backend.dm.logger.Logger;
import backend.dm.page.Page;
import backend.dm.page.PageNormal;
import backend.tm.TransactionManager;
import com.google.common.primitives.Bytes;
import utils.Panic;
import utils.Parser;

import java.util.*;

/**
 * 数据库的崩溃恢复
 *
 * 日志策略: 更新数据文件前必须先进行对应的日志操作, 保证日志写入磁盘后, 再进行数据操作
 *     - 插入操作日志: (LogType, TransactionId, Position, Data)
 *     - 更新操作日志: (LogType, TransactionId, UID, Position, OldData, NewData)
 *
 * REDO: 正序扫描事务的日志, 执行日志文件中的数据操作
 * UNDO: 倒序扫描事务的日志, 执行日志文件中的数据逆操作, 插入的逆操作是删除, 但数据库没有真正的数据删除操作, 只是修改其中的标志位
 *
 * 数据恢复:
 *     - 单线程事务: 各事务之间的日志不会相交, 按顺序存储
 *     - 多线程事务: 为保证多线程事务恢复的正确性, 需要满足以下几个规定
 *          1. 正在进行的事务, 不会读取其他任何**未提交事务**产生的数据
 *          2. 正在修改的事务, 不会修改其他任何**未提交的事务**产生的数据
 */
public class Recover {

    private static final byte INSERT_LOG_FLAG = 0;
    private static final byte UPDATE_LOG_FLAG = 1;

    private static final int REDO_FLAG = 0;
    private static final int UNDO_FLAG = 1;

    /**
     * InsertLog: [LogType, XID, PageNo, Position, Data]
     */
    private static final int TYPE_OFFSET = 0;                             // type: byte
    private static final int XID_OFFSET = TYPE_OFFSET + 1;                // xid: long
    private static final int PAGE_NO_OFFSET = XID_OFFSET + 8;             // pageNo: int
    private static final int POSITION_OFFSET = PAGE_NO_OFFSET + 4;        // position: byte
    private static final int INSERT_DATA_OFFSET = POSITION_OFFSET + 2;

    /**
     * UpdateLog: [LogType, XID, UID, OldData, NewData]
     */
    private static final int UPDATE_UID_OFFSET = XID_OFFSET + 8;          // uid: long
    private static final int UPDATE_DATA_OFFSET = UPDATE_UID_OFFSET + 8;



    static class InsertLogInfo {
        long xid;
        int pageNo;
        short position;
        byte[] data;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNo;
        short position;
        byte[] oldData;
        byte[] newData;
    }


    /**
     * REDO: 事务处于非运行状态, 恢复时则重做
     *
     * @param tm
     * @param logger
     * @param pageCache
     */
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();   // 定位到日志开始位置
        while (true) {
            byte[] log = logger.next();    // 获取去除描述信息的log
            if (log == null) {
                break;
            }

            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;

                if (!tm.isActive(xid)) {
                    // TODO: redo
                    doInsertLog(pageCache, log, REDO_FLAG);
                }
            }
            else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;

                if (!tm.isActive(xid)) {
                    // TODO: redo
                    doUpdateLog(pageCache, log, REDO_FLAG);
                }
            }
        }
    }

    /**
     * UNDO: 事务处于运行状态, 恢复时则回滚
     *
     * @param tm
     * @param logger
     * @param pageCache
     */
    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();

        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }

            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;

                if (tm.isAborted(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }

                    logCache.get(xid).add(log);
                }
            }
            else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;

                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }

                    logCache.get(xid).add(log);
                }
            }
        }

        // 倒序 UNDO
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    // TODO: undo
                    doInsertLog(pageCache, log, UNDO_FLAG);
                }
                else {
                    // TODO: undo
                    doUpdateLog(pageCache, log, UNDO_FLAG);
                }
            }

            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == INSERT_LOG_FLAG;
    }

    private static boolean isUpdateLog(byte[] log) {
        return log[0] == UPDATE_LOG_FLAG;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo info = new InsertLogInfo();

        info.xid = Parser.parseBytesToLong(Arrays.copyOfRange(log, XID_OFFSET, PAGE_NO_OFFSET));
        info.pageNo = Parser.parseBytesToInt(Arrays.copyOfRange(log, PAGE_NO_OFFSET, POSITION_OFFSET));
        info.position = Parser.parseBytesToShort(Arrays.copyOfRange(log, POSITION_OFFSET, INSERT_DATA_OFFSET));
        info.data = Arrays.copyOfRange(log, INSERT_DATA_OFFSET, log.length);

        return info;
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo info = new UpdateLogInfo();

        info.xid = Parser.parseBytesToLong(Arrays.copyOfRange(log, XID_OFFSET, UPDATE_UID_OFFSET));

        long uid = Parser.parseBytesToLong(Arrays.copyOfRange(log, UPDATE_UID_OFFSET, UPDATE_DATA_OFFSET));
        info.position = (short) (uid & ((1L << 16)) - 1);
        uid >>>= 32;
        info.pageNo = (int) (uid & ((1L << 32) - 1));

        int length = (log.length - UPDATE_DATA_OFFSET) / 2;
        info.oldData = Arrays.copyOfRange(log, UPDATE_DATA_OFFSET, UPDATE_DATA_OFFSET + length);
        info.newData = Arrays.copyOfRange(log, UPDATE_DATA_OFFSET + length, UPDATE_DATA_OFFSET + 2 * length);

        return info;
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo info = parseInsertLog(log);
        Page page = null;

        try {
            page = pageCache.getPage(info.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            if (flag == REDO_FLAG) {
                DataItem.setDataItemInvalid(info.data);
            }

            PageNormal.recoverInsert(page, info.data, info.position);
        } finally {
            page.release();    // 引用减1
        }
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        UpdateLogInfo info = parseUpdateLog(log);
        int pageNo = info.pageNo;
        short position = info.position;

        byte[] data = null;
        if (flag == REDO_FLAG) {
            data = info.newData;
        }
        else {
            data = info.oldData;
        }

        Page page = null;
        try {
            page = pageCache.getPage(info.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            PageNormal.recoverUpdate(page, data, position);
        } finally {
            page.release();
        }
    }

    /**
     * 插入日志
     * InsertLog: [LogType(1), XID(8), PageNo(4), Position(2), Data]
     * @return
     */
    public static byte[] insertLog(long xid, Page page, byte[] data) {
        byte[] logTypeRaw = {INSERT_LOG_FLAG};
        byte[] xidRaw = Parser.parseLongToBytes(xid);
        byte[] pageNoRaw = Parser.parseIntToBytes(page.getPageNo());
        byte[] positionRaw = Parser.parseShortToBytes(PageNormal.getFSO(page));    // 获取当前页指针指向的空闲位置

        return Bytes.concat(logTypeRaw, xidRaw, pageNoRaw, positionRaw, data);
    }

    /**
     * 更新日志
     * UpdateLog: [LogType, XID, UID, OldData, NewData]
     * @return
     */
    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logTypeRaw = {UPDATE_LOG_FLAG};
        byte[] xidRaw = Parser.parseLongToBytes(xid);
        byte[] uidRaw = Parser.parseLongToBytes(dataItem.getUid());
        byte[] oldData = dataItem.getOldData();

        SubArray data = dataItem.getData();
        byte[] newData = Arrays.copyOfRange(data.getArray(), data.getStart(), data.getEnd());

        return Bytes.concat(logTypeRaw, xidRaw, uidRaw, oldData, newData);
    }
}
