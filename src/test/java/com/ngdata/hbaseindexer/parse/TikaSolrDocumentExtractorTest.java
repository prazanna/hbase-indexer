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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.ngdata.hbaseindexer.parse.extract.SingleCellExtractor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class TikaSolrDocumentExtractorTest {

    @Test
    public void testExtractDocument() throws IOException {

        byte[] columnFamily = Bytes.toBytes("cf");
        byte[] columnQualifier = Bytes.toBytes("qualifier");
        final String applicableValue = "this is the test data";
        final String nonApplicableValue = "not-applicable value";
        KeyValue applicableKeyValue = new KeyValue(Bytes.toBytes("row"), columnFamily, columnQualifier,
                Bytes.toBytes(applicableValue));
        KeyValue nonApplicableKeyValue = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("other cf"), columnQualifier,
                Bytes.toBytes(nonApplicableValue));
        Result result = new Result(new KeyValue[] { applicableKeyValue, nonApplicableKeyValue });

        SolrDocumentExtractor documentExtractor = TikaSolrDocumentExtractor.createInstance(new SingleCellExtractor(
                columnFamily, columnQualifier), "prefix_", "text/plain");
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        documentExtractor.extractDocument(result, solrInputDocument);

        // Make sure that the input text got in here somehow
        assertTrue(solrInputDocument.containsKey("prefix_content"));
        assertTrue(solrInputDocument.get("prefix_content").getValues().toString().contains(applicableValue));

        assertFalse(solrInputDocument.get("prefix_content").getValues().toString().contains(nonApplicableValue));
    }

}
