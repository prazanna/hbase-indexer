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
package com.ngdata.hbaseindexer.parse.extract;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class AbstractPrefixMatchingExtractorTest {
    
    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] COLFAM = Bytes.toBytes("cf");
    
    private AbstractPrefixMatchingExtractor extractor;
    
    @Before
    public void setUp() {
        extractor = new DummyPrefixMatchingExtractor(COLFAM, Bytes.toBytes("AB"));
    }
    

    @Test
    public void testExtract_NoKeyValues() {
        Result result = new Result();
        Collection<byte[]> extracted = extractor.extract(result);
        
        assertTrue(extracted.isEmpty());
    }
    
    @Test
    public void testExtract_IncludeAll() {
        Result result = new Result(new KeyValue[]{
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ABB"), Bytes.toBytes("value ABB")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ABC"), Bytes.toBytes("value ABC"))
        });
        
        assertExtractEquals(Lists.newArrayList("ABB:value ABB", "ABC:value ABC"), extractor.extract(result));
    }
    
    @Test
    public void testExtract_IncludeBottomHalf() {
        Result result = new Result(new KeyValue[]{
                new KeyValue(ROW, COLFAM, Bytes.toBytes("AB"), Bytes.toBytes("value AB")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ABB"), Bytes.toBytes("value ABB")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("AC"), Bytes.toBytes("value AC"))
        });
        
        assertExtractEquals(Lists.newArrayList("AB:value AB", "ABB:value ABB"), extractor.extract(result));
    }
    
    @Test
    public void testExtract_IncludeTopHalf() {
        Result result = new Result(new KeyValue[]{
                new KeyValue(ROW, COLFAM, Bytes.toBytes("A"), Bytes.toBytes("value A")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("AB"), Bytes.toBytes("value AB")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ABB"), Bytes.toBytes("value ABB"))
        });
        
        assertExtractEquals(Lists.newArrayList("AB:value AB", "ABB:value ABB"), extractor.extract(result));
    }
    
    @Test
    public void testExtract_IncludeMiddle() {
        Result result = new Result(new KeyValue[]{
                new KeyValue(ROW, COLFAM, Bytes.toBytes("AAC"), Bytes.toBytes("value AAC")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ABC"), Bytes.toBytes("value ABC")),
                new KeyValue(ROW, COLFAM, Bytes.toBytes("ACC"), Bytes.toBytes("value ACC"))
        });
        
        assertExtractEquals(Lists.newArrayList("ABC:value ABC"), extractor.extract(result));
    }
    
    @Test
    public void testIsApplicable_ExactMatch() {
        assertTrue(extractor.isApplicable(new KeyValue(ROW, COLFAM, Bytes.toBytes("AB"), Bytes.toBytes("value"))));
    }
    
    @Test
    public void testIsApplicable_WrongColumnFamily() {
        assertFalse(extractor.isApplicable(new KeyValue(ROW, Bytes.toBytes("wrong family"), Bytes.toBytes("AB"), Bytes.toBytes("value"))));
    }
    
    @Test
    public void testIsApplicable_PrefixMatch() {
        assertTrue(extractor.isApplicable(new KeyValue(ROW, COLFAM, Bytes.toBytes("ABC"), Bytes.toBytes("value"))));
    }
    
    @Test
    public void testIsApplicable_NotMatch() {
        assertFalse(extractor.isApplicable(new KeyValue(ROW, COLFAM, Bytes.toBytes("no match"), Bytes.toBytes("value"))));
    }
    
    private void assertExtractEquals(List<String> expected, Collection<byte[]> actual) {
        List<String> actualStrings = Lists.newArrayList();
        for (byte[] byteArray : actual) {
            actualStrings.add(Bytes.toString(byteArray));
        }
        assertEquals(expected, actualStrings);
    }
    
    static class DummyPrefixMatchingExtractor extends AbstractPrefixMatchingExtractor {

        DummyPrefixMatchingExtractor(byte[] columnFamily, byte[] prefix) {
            super(columnFamily, prefix);
        }

        @Override
        protected byte[] extractInternal(byte[] qualifier, byte[] value) {
            return Bytes.toBytes(String.format("%s:%s", Bytes.toStringBinary(qualifier), Bytes.toStringBinary(value)));
        }
        
    }

}
