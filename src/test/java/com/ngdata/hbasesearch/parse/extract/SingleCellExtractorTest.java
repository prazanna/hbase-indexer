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
package com.ngdata.hbasesearch.parse.extract;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class SingleCellExtractorTest {

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("col_family");
    private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("col_qualifier");

    private SingleCellExtractor extractor;

    @Before
    public void setUp() {
        extractor = new SingleCellExtractor(COLUMN_FAMILY, COLUMN_QUALIFIER);
    }

    @Test
    public void testExtract() {
        Result result = mock(Result.class);
        byte[] value = new byte[] { 1, 2, 3 };
        when(result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)).thenReturn(value);

        assertEquals(ImmutableList.of(value), extractor.extract(result));
    }

    @Test
    public void testExtract_CellNotPresent() {
        Result result = mock(Result.class);
        when(result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)).thenReturn(null);

        assertEquals(Collections.emptyList(), extractor.extract(result));
    }
    
}
