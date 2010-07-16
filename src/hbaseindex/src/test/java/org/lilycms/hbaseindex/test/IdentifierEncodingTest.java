package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilycms.hbaseindex.IdentifierEncoding;

import static org.junit.Assert.*;

public class IdentifierEncodingTest {
    @Test
    public void test() {
        byte[] key = Bytes.toBytes("foobar");
        assertEquals("foobar", Bytes.toString(IdentifierEncoding.decode(IdentifierEncoding.encode(key), false)));
    }
}
