package com.alibaba.flink.connectors.dts.util;

import java.io.Closeable;

public class DtsUtil {
    public static  void swallowErrorClose(Closeable target) {
        try {
            if (null != target) {
                target.close();
            }
        } catch (Exception e) {
        }
    }

    public static String composeCheckpint(long offset, long timestamp) {
        return String.valueOf(offset) + "@" + String.valueOf(timestamp);
    }

    public static long getOffsetFromCheckpint(String checkpoint) {
        return Long.valueOf(checkpoint.substring(0, checkpoint.indexOf("@")));
    }

    public static long getTimestampFromCheckpint(String checkpoint) {
        return Long.valueOf(checkpoint.substring(checkpoint.indexOf("@") + 1));
    }

    public static long getTimestampSeconds(long ts) {
        return ts / 10000000000L > 0 ? ts/1000 : ts;
    }
}
