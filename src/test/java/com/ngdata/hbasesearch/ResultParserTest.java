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
package com.ngdata.hbasesearch;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ngdata.hbasesearch.FieldDefinition.ValueSource;
import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class ResultParserTest {

    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] COLUMN_FAMILY_A = Bytes.toBytes("cfA");
    private static final byte[] COLUMN_FAMILY_B = Bytes.toBytes("cfB");
    private static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
    private static final byte[] QUALIFIER_B = Bytes.toBytes("qualifierB");

    @Test
    public void testParse() {
        FieldDefinition fieldDefA = new FieldDefinition("fieldA", "cfA:qualifierA", ValueSource.VALUE, "int");
        FieldDefinition fieldDefB = new FieldDefinition("fieldB", "cfB:qualifierB", ValueSource.VALUE,
                DummyValueMapper.class.getName());
        ResultParser resultParser = ResultParser.newInstance(Lists.newArrayList(fieldDefA, fieldDefB));

        KeyValue kvA = new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes(42));
        KeyValue kvB = new KeyValue(ROW, COLUMN_FAMILY_B, QUALIFIER_B, "dummy value".getBytes());
        Result result = new Result(Lists.newArrayList(kvA, kvB));

        Multimap<String, Object> parsedOutput = resultParser.parse(result);

        Multimap<String, Object> expectedOutput = ArrayListMultimap.create();
        expectedOutput.put("fieldA", 42);
        expectedOutput.put("fieldB", "A");
        expectedOutput.put("fieldB", "B");
        expectedOutput.put("fieldB", "C");

        assertEquals(expectedOutput, parsedOutput);
    }

    public static class DummyValueMapper implements ByteArrayValueMapper {

        @Override
        public Collection<Object> map(byte[] input) {
            return Lists.<Object> newArrayList("A", "B", "C");
        }

    }
}
