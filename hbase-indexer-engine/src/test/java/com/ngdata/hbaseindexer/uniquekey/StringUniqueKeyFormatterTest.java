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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class StringUniqueKeyFormatterTest extends BaseUniqueKeyFormatterTest {

    @Override
    protected UniqueKeyFormatter createFormatter() {
        return new StringUniqueKeyFormatter();
    }

    @Test
    public void testFormatRow_SimpleString() {
        assertEquals("row", createFormatter().formatRow(Bytes.toBytes("row")));
    }

    @Test
    public void testFormatKeyValue_SimpleString() {
        assertEquals(
                "row-column-qualifier",
                createFormatter().formatKeyValue(
                        new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("column"), Bytes.toBytes("qualifier"))));
    }

}
