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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ngdata.hbasesearch.conf.FieldDefinition;
import com.ngdata.hbasesearch.conf.FieldDefinition.ValueSource;
import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;

public class ResultToSolrMapperTest {

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
        ResultToSolrMapper resultMapper = new ResultToSolrMapper(Lists.newArrayList(fieldDefA, fieldDefB));

        KeyValue kvA = new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes(42));
        KeyValue kvB = new KeyValue(ROW, COLUMN_FAMILY_B, QUALIFIER_B, "dummy value".getBytes());
        Result result = new Result(Lists.newArrayList(kvA, kvB));

        Multimap<String, Object> parsedOutput = resultMapper.parse(result);

        Multimap<String, Object> expectedOutput = ArrayListMultimap.create();
        expectedOutput.put("fieldA", 42);
        expectedOutput.put("fieldB", "A");
        expectedOutput.put("fieldB", "B");
        expectedOutput.put("fieldB", "C");

        assertEquals(expectedOutput, parsedOutput);
    }

    @Test
    public void testIsRelevantKV_WithoutWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:qualifier", ValueSource.VALUE, "int");
        ResultToSolrMapper resultMapper = new ResultToSolrMapper(Lists.newArrayList(fieldDef));

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier"),
                Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("wrongqualifier"), Bytes.toBytes("value"));
        
        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }

    @Test
    public void testIsRelevantKV_WithWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:quali*", ValueSource.VALUE, "int");
        ResultToSolrMapper resultMapper = new ResultToSolrMapper(Lists.newArrayList(fieldDef));

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("qu wrong qualifier"), Bytes.toBytes("value"));

        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }


    @Test
    public void testMap() {
        FieldDefinition fieldDefA = new FieldDefinition("fieldA", "cfA:qualifierA", ValueSource.VALUE, "int");
        ResultToSolrMapper resultMapper = new ResultToSolrMapper(Lists.newArrayList(fieldDefA));

        KeyValue kvA = new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes(42));
        Result result = new Result(Lists.newArrayList(kvA));

        SolrInputDocument solrDocument = resultMapper.map(result);

        assertEquals(1, solrDocument.getFieldNames().size());
        SolrInputField field = solrDocument.getField("fieldA");
        assertEquals(Lists.newArrayList(42), field.getValues());
    }

    public static class DummyValueMapper implements ByteArrayValueMapper {

        @Override
        public Collection<Object> map(byte[] input) {
            return Lists.<Object> newArrayList("A", "B", "C");
        }

    }
}
