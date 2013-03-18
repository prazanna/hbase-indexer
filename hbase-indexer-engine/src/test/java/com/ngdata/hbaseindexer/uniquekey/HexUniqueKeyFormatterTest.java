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
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;

public class HexUniqueKeyFormatterTest extends BaseUniqueKeyFormatterTest {

    @Override
    protected UniqueKeyFormatter createFormatter() {
        return new HexUniqueKeyFormatter();
    }

    @Test
    public void testFormatRow_SimpleHex() {
        assertEquals("0102", createFormatter().formatRow(new byte[] { 1, 2 }));
    }

    @Test
    public void testFormatKeyValue_SimpleHex() {
        assertEquals(
                "a1-b1-c1",
                createFormatter().formatKeyValue(
                        new KeyValue(new byte[] { (byte)161 }, new byte[] { (byte)177 }, new byte[] { (byte)193 })));
    }

    @Test
    public void testEncodeAsString_SimpeCase() {
        HexUniqueKeyFormatter formatter = new HexUniqueKeyFormatter();
        assertEquals("010203ff", formatter.encodeAsString(new byte[] { 1, 2, 3, (byte)255 }));
    }

    @Test
    public void testDecodeFromString_SimpleCase() {
        HexUniqueKeyFormatter formatter = new HexUniqueKeyFormatter();
        assertArrayEquals(new byte[] { 1, 2, 3, (byte)255 }, formatter.decodeFromString("010203ff"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeFromString_NonHexString() {
        HexUniqueKeyFormatter formatter = new HexUniqueKeyFormatter();
        formatter.decodeFromString("xyz");
    }

}
