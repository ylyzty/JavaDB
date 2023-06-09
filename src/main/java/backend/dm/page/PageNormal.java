package backend.dm.page;

import backend.dm.PageCache.PageCache;
import utils.Parser;

import java.util.Arrays;

/**
 * 普通数据页
 *
 * [0:1]: 存储空闲位置偏移
 * [2:]: 存储真实数据
 */
public class PageNormal {

    private static final short FREE_OFFSET = 0;
    private static final short DATA_OFFSET = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - DATA_OFFSET;    // 最多可存储字节数

    public static byte[] initData() {
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setFSO(data, DATA_OFFSET);

        return data;
    }

    public static short insert(Page page, byte[] insertData) {
        page.setDirty(true);

        short offset = getFSO(page);
        System.arraycopy(insertData, 0, page.getData(), offset, insertData.length);
        setFSO(page.getData(), (short) (offset + insertData.length));

        return offset;
    }


    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }
    public static short getFSO(byte[] data) {
        return Parser.parseShort(Arrays.copyOfRange(data, FREE_OFFSET, DATA_OFFSET));
    }
    public static void setFSO(byte[] data, short offset) {
        System.arraycopy(Parser.parseShortToByte(offset), 0, data, FREE_OFFSET, DATA_OFFSET - FREE_OFFSET);
    }

    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int)getFSO(page);
    }

    /**
     * 数据库崩溃后重新打开时, 直接插入数据
     * @param page
     * @param data
     * @param offset
     */
    public static void recoverInsert(Page page, byte[] data, short offset) {
        page.setDirty(true);
        System.arraycopy(data, 0, page.getData(), offset, data.length);

        short pageFSO = getFSO(page.getData());
        if (pageFSO < offset + data.length) {
            setFSO(page.getData(), (short) (offset + data.length));
        }
    }

    /**
     * 数据库崩溃后重新打开时, 修改数据
     * @param page
     * @param data
     * @param offset
     */
    public static void recoverUpdate(Page page, byte[] data, short offset) {
        page.setDirty(true);
        System.arraycopy(data, 0, page.getData(), offset, data.length);
    }

}
