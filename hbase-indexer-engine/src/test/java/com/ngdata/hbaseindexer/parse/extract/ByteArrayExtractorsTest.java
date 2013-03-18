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

import static com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class ByteArrayExtractorsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGetExtractor_QualifierBasedWithoutWildcard() {
        // It doesn't make sense to not use a wildcard if we're extracting the qualifier
        ByteArrayExtractors.getExtractor("colfam:qualifier", ValueSource.QUALIFIER);
    }

    @Test
    public void testGetExtractor_Value_WithWildcard() {
        ByteArrayExtractor extractor = ByteArrayExtractors.getExtractor("colfam:quali*", ValueSource.VALUE);
        assertTrue(extractor instanceof PrefixMatchingCellExtractor);
        assertArrayEquals(Bytes.toBytes("quali"), ((PrefixMatchingCellExtractor)extractor).getPrefix());
    }

    @Test
    public void testGetExtractor_Qualifier_WithWildcard() {
        ByteArrayExtractor extractor = ByteArrayExtractors.getExtractor("colfam:quali*", ValueSource.QUALIFIER);
        assertTrue(extractor instanceof PrefixMatchingQualifierExtractor);
        assertArrayEquals(Bytes.toBytes("quali"), ((PrefixMatchingQualifierExtractor)extractor).getPrefix());
    }

    @Test
    public void testGetExtractor_CellValue_NoWildcard() {
        ByteArrayExtractor extractor = ByteArrayExtractors.getExtractor("colfam:qualifier", ValueSource.VALUE);
        assertTrue(extractor instanceof SingleCellExtractor);
    }

    @Test
    public void testGetFamily() {
        assertEquals("colfam", ByteArrayExtractors.getFamily("colfam:qualifier"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetFamily_NoColon() {
        ByteArrayExtractors.getFamily("colfam");
    }

    @Test
    public void testGetQualifier() {
        assertEquals("qualifier", ByteArrayExtractors.getQualifier("colfam:qualifier"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetQualifier_NoColon() {
        ByteArrayExtractors.getQualifier("qualifier");
    }

    @Test
    public void testIsWildcard_True() {
        assertTrue(ByteArrayExtractors.isWildcard("qualifi*"));
    }

    @Test
    public void testIsWildcard_False() {
        assertFalse(ByteArrayExtractors.isWildcard("qualifier"));
    }

}
