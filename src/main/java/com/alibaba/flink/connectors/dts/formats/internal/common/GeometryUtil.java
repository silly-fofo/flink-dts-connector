package com.alibaba.flink.connectors.dts.formats.internal.common;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import java.nio.ByteBuffer;

/** GeometryUtil is a tool class to convert Geometry bytes to String. */
public class GeometryUtil {

    private static final double SCALE = Math.pow(10.0D, 4.0);

    public static String fromWKBToWKTText(ByteBuffer data) throws ParseException {
        if (null == data) {
            return null;
        } else {
            WKBReader reader = new WKBReader();
            Geometry geometry = reader.read(data.array());
            return geometry.toText();
        }
    }
}
