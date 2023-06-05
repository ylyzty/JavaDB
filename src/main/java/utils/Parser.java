package utils;

import java.nio.ByteBuffer;

public class Parser {
    public static long parseLong(byte[] buf) {
        // 读入8个字节数据, 转换为 long 类型
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 8);
        return byteBuffer.getLong();
    }

    public static byte[] parseLongToByte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
