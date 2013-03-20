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
package com.ngdata.hbaseindexer.parse;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class HBaseSolrDocumentExtractorTest extends TestCase {

    private ByteArrayExtractor valueExtractor;
    private ByteArrayValueMapper valueMapper;
    private HBaseSolrDocumentExtractor documentExtractor;

    @Override
    @Before
    public void setUp() {
        valueExtractor = mock(ByteArrayExtractor.class);
        valueMapper = mock(ByteArrayValueMapper.class);
        documentExtractor = new HBaseSolrDocumentExtractor("fieldName", valueExtractor, valueMapper);
    }

    @Test
    public void testExtractDocument() {
        byte[] bytesA = new byte[] { 1, 2 };
        byte[] bytesB = new byte[] { 3, 4 };

        Result result = mock(Result.class);

        when(valueExtractor.extract(result)).thenReturn(Lists.newArrayList(bytesA, bytesB));
        doReturn(Lists.newArrayList("A")).when(valueMapper).map(bytesA);
        doReturn(Lists.newArrayList("B")).when(valueMapper).map(bytesB);
        
        SolrInputDocument solrDocument = new SolrInputDocument();
        documentExtractor.extractDocument(result, solrDocument);
        
        assertEquals(Sets.newHashSet("fieldName"), solrDocument.keySet());
        assertEquals(Lists.newArrayList("A", "B"), solrDocument.get("fieldName").getValues());
    }

}
