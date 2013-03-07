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
package com.ngdata.hbaseindexer;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UniqueKeyFormatterTest {
    @Test
    public void testStringFormatter() {
        UniqueKeyFormatter formatter = new StringUniqueKeyFormatter();
        assertEquals("row", formatter.format(Bytes.toBytes("row")));
        assertEquals("row-column-qualifier", formatter.format(Bytes.toBytes("row"), Bytes.toBytes("column"),
                Bytes.toBytes("qualifier")));
    }

    @Test
    public void testHexFormatter() {
        UniqueKeyFormatter formatter = new HexUniqueKeyFormatter();
        assertEquals("0102", formatter.format(new byte[] {1, 2}));
        assertEquals("a1-b1-c1", formatter.format(new byte[] {(byte)161}, new byte[] {(byte)177}, new byte[] {(byte)193}));
    }
}
