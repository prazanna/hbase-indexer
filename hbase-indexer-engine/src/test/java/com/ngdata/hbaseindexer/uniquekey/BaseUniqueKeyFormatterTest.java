/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.uniquekey;

import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public abstract class BaseUniqueKeyFormatterTest {

    protected abstract UniqueKeyFormatter createFormatter();

    protected void doRoundTripRowKey(byte[] rowKey) {
        UniqueKeyFormatter formatter = createFormatter();
        String formatted = formatter.formatRow(rowKey);
        byte[] unformatted = formatter.unformatRow(formatted);
        assertArrayEquals(rowKey, unformatted);
    }
    
    protected void doRoundTripColumnFamily(byte[] columnFamily) {
        UniqueKeyFormatter formatter = createFormatter();
        String formatted = formatter.formatRow(columnFamily);
        byte[] unformatted = formatter.unformatRow(formatted);
        assertArrayEquals(columnFamily, unformatted);
    }

    protected void doRoundTrip(KeyValue keyValue) {
        UniqueKeyFormatter formatter = createFormatter();
        String formatted = formatter.formatKeyValue(keyValue);
        KeyValue unformatted = formatter.unformatKeyValue(formatted);
        assertArrayEquals(keyValue.getRow(), unformatted.getRow());
        assertArrayEquals(keyValue.getFamily(), unformatted.getFamily());
        assertArrayEquals(keyValue.getQualifier(), unformatted.getQualifier());
    }

    @Test(expected = NullPointerException.class)
    public void testFormatRow_Null() {
        createFormatter().formatRow(null);
    }
    
    @Test(expected = NullPointerException.class)
    public void testFormatFamily_Null() {
        createFormatter().formatFamily(null);
    }

    @Test(expected = NullPointerException.class)
    public void testFormatKeyValue_NullRowKey() {
        createFormatter().formatKeyValue(new KeyValue());
    }

    @Test
    public void testFormatRow_SimpleCase() {
        doRoundTripRowKey(new byte[]{1,2,3});
    }
    
    @Test
    public void testFormatFamily_SimpleCase() {
        doRoundTripColumnFamily(new byte[]{1,2,3});
    }

    @Test
    public void testFormatKeyValue_SimpleCase() {
        doRoundTrip(new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier")));
    }

    @Test
    public void testFormatRow_WithHyphens() {
        doRoundTripRowKey(Bytes.toBytes("key-with-hyphens"));
    }
    
    @Test
    public void testFormatFamily_WithHyphens() {
        doRoundTripColumnFamily(Bytes.toBytes("family-with-hyphens"));
    }

    @Test
    public void testFormatKeyValue_WithHyphens() {
        doRoundTrip(new KeyValue(Bytes.toBytes("ro-w"), Bytes.toBytes("c-f"), Bytes.toBytes("quali-fier")));
    }

}
