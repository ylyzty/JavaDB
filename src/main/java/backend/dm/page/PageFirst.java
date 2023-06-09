package backend.dm.page;

import backend.dm.PageCache.PageCache;
import utils.RandomUtil;

import java.util.Arrays;

/**
 * 数据库存储的第一页
 *
 * 特殊用途: 存储元数据, 启动检查 ValidCheck
 */
public class PageFirst {
    private static final int VC_LENGTH = 8;
    private static final int VC_OFFSET = 100;

    private static byte[] initData() {
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setVcOpen(data);

        return data;
    }

    /**
     * 数据库开启时自动在 100~107 字节处保存一串随机数
     * @param page
     */
    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    public static void setVcOpen(byte[] data) {
        System.arraycopy(RandomUtil.randomBytes(VC_LENGTH), 0, data, VC_OFFSET, VC_LENGTH);
    }

    /**
     * 数据库关闭时自动拷贝上述字节到 108~115 处
     * @param page
     */
    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    public static void setVcClose(byte[] data) {
        System.arraycopy(data, VC_OFFSET, data, VC_OFFSET + VC_LENGTH, VC_LENGTH);
    }

    public static boolean checkVc(Page page) {
        return checkVc(page.getData());
    }

    public static boolean checkVc(byte[] data) {
        return Arrays.equals(Arrays.copyOfRange(data, VC_OFFSET, VC_OFFSET + VC_LENGTH),
                Arrays.copyOfRange(data, VC_OFFSET + VC_LENGTH, VC_OFFSET + VC_LENGTH * 2));
    }
}
