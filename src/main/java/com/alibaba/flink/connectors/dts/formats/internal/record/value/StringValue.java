package com.alibaba.flink.connectors.dts.formats.internal.record.value;

import org.apache.commons.lang3.StringUtils;
import com.alibaba.flink.connectors.dts.formats.internal.common.BytesUtil;
import com.alibaba.flink.connectors.dts.formats.internal.common.JDKCharsetMapper;
import com.alibaba.flink.connectors.dts.formats.internal.common.SwallowException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/** StringValue. */
public class StringValue implements Value<ByteBuffer> {

    public static final String DEFAULT_CHARSET = "UTF-8";
    private ByteBuffer data;
    private String charset;
    private static ThreadLocal<Map<String, Charset>> charsetMap = new ThreadLocal<>();

    public StringValue(ByteBuffer data, String charset) {
        this.data = data;
        this.charset = charset;
    }

    public StringValue(String data) {
        this(
                ByteBuffer.wrap(
                        SwallowException.callAndThrowRuntimeException(
                                () -> data.getBytes(DEFAULT_CHARSET))),
                DEFAULT_CHARSET);
    }

    public String getCharset() {
        return this.charset;
    }

    @Override
    public ValueType getType() {
        return ValueType.STRING;
    }

    @Override
    public ByteBuffer getData() {
        return this.data;
    }

    @Override
    public String toString() {

        // just return hex string if missing charset
        if (StringUtils.isEmpty(charset)) {
            return BytesUtil.byteBufferToHexString(data);
        }

        // try encode data by specified charset
        Map<String, Charset> localMap = charsetMap.get();
        if (null == localMap) {
            localMap = new HashMap<>();
            charsetMap.set(localMap);
        }
        try {
            Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(charset));
            return new String(data.array(), charsetObject);
        } catch (Exception e1) {
            try {
                Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(JDKCharsetMapper.getJDKECharset(charset)));
                return new String(data.array(), charsetObject);
            } catch (Exception e2) {
                return charset + "_'" + BytesUtil.byteBufferToHexString(data) + "'";
            }
        }
    }


    @Override
    public long size() {
        if (null != data) {
            return data.capacity();
        }

        return 0L;
    }
}
