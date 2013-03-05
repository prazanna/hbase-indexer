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

import com.google.common.collect.Lists;
import com.ngdata.hbasesearch.conf.IndexConf;
import com.ngdata.hbasesearch.conf.IndexConfBuilder;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IndexerTest {
    /**
     * When receiving an event for a different table than the one specified in the IndexConf, the Indexer
     * should not interact with HBase or Solr.
     */
    @Test
    public void testNonmatchedTable() {
        IndexConf conf = new IndexConfBuilder().table("table1").create();

        HTablePool tablePool = mock(HTablePool.class);
        SolrServer solrServer = mock(SolrServer.class);
        Indexer indexer = new Indexer(conf, null, null, tablePool, solrServer);

        SepEvent event = new SepEvent(Bytes.toBytes("table2"), null, null, null);
        indexer.processEvent(event);

        verifyZeroInteractions(tablePool, solrServer);
    }

    /**
     * When we get a sep event for a row which does not exist anymore (or at least, doesn't contain any
     * relevant columns), then we expect a delete on Solr.
     */
    @Test
    public void testNonExistingRow() throws Exception {
        IndexConf conf = new IndexConfBuilder().table("table1").create();

        HTableInterface table = mock(HTableInterface.class);
        when(table.get(any(Get.class))).thenReturn(new Result());

        HTablePool tablePool = mock(HTablePool.class);
        when(tablePool.getTable(Bytes.toBytes("table1"))).thenReturn(table);
        when(tablePool.getTable("table1")).thenReturn(table);

        SolrServer solrServer = mock(SolrServer.class);

        HBaseToSolrMapper mapper = mock(HBaseToSolrMapper.class);
        when(mapper.isRelevantKV(any(KeyValue.class))).thenReturn(true);
        Indexer indexer = new Indexer(conf, mapper, new DefaultSolrUniqueKeyFormatter(), tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue());
        SepEvent event = new SepEvent(Bytes.toBytes("table1"), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        verify(solrServer).deleteById("row1");
        verifyNoMoreInteractions(solrServer);
    }

    @Test
    public void testRowBasedIndexing() throws Exception {
        IndexConf conf = new IndexConfBuilder().table("table1").create();

        HBaseToSolrMapper mapper = new HBaseToSolrMapper() {
            @Override
            public boolean isRelevantKV(KeyValue kv) {
                return true;
            }

            @Override
            public Get getGet(byte[] row) {
                return new Get(row);
            }

            @Override
            public SolrInputDocument map(Result result) {
                SolrInputDocument doc = new SolrInputDocument();
                return doc;
            }
        };

        HTableInterface table = mock(HTableInterface.class);
        when(table.get(any(Get.class))).thenReturn(new Result(Lists.newArrayList(new KeyValue())));

        HTablePool tablePool = mock(HTablePool.class);
        when(tablePool.getTable(Bytes.toBytes("table1"))).thenReturn(table);
        when(tablePool.getTable("table1")).thenReturn(table);

        SolrServer solrServer = mock(SolrServer.class);

        Indexer indexer = new Indexer(conf, mapper, new DefaultSolrUniqueKeyFormatter(), tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue());
        SepEvent event = new SepEvent(Bytes.toBytes("table1"), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer).add(arg.capture());
        assertEquals("row1", arg.getValue().getFieldValue("id"));
    }

    @Test
    public void testColumnBasedIndexing() throws Exception {
        IndexConf conf = new IndexConfBuilder().table("table1").mappingType(IndexConf.MappingType.COLUMN).create();

        HBaseToSolrMapper mapper = new HBaseToSolrMapper() {
            @Override
            public boolean isRelevantKV(KeyValue kv) {
                return Bytes.toString(kv.getFamily()).equals("messages");
            }

            @Override
            public Get getGet(byte[] row) {
                return new Get(row);
            }

            @Override
            public SolrInputDocument map(Result result) {
                SolrInputDocument doc = new SolrInputDocument();
                return doc;
            }
        };

        SolrServer solrServer = mock(SolrServer.class);

        Indexer indexer = new Indexer(conf, mapper, new DefaultSolrUniqueKeyFormatter(), null, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(
                new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"), Bytes.toBytes("msg1"), Bytes.toBytes("the message")),
                new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"), Bytes.toBytes("msg2"), Bytes.toBytes("another message")));

        SepEvent event = new SepEvent(Bytes.toBytes("table1"), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer, times(2)).add(arg.capture());
        List<SolrInputDocument> docs = arg.getAllValues();
        assertEquals("row1-messages-msg1", docs.get(0).getFieldValue("id"));
        assertEquals("row1-messages-msg2", docs.get(1).getFieldValue("id"));
    }
}
