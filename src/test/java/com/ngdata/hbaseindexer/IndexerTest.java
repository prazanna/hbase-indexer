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

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.IndexConf;
import com.ngdata.hbaseindexer.conf.IndexConf.RowReadMode;
import com.ngdata.hbaseindexer.conf.IndexConfBuilder;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class IndexerTest {

    private static final String TABLE_A = "_table_a_";
    private static final String TABLE_B = "_table_b_";

    private SolrServer solrServer;
    private HTablePool tablePool;
    private HTableInterface tableA;
    private HTableInterface tableB;

    @Before
    public void setUp() {
        solrServer = mock(SolrServer.class);
        tablePool = mock(HTablePool.class);
        tableA = mock(HTableInterface.class);
        tableB = mock(HTableInterface.class);
        when(tablePool.getTable(TABLE_A)).thenReturn(tableA);
        when(tablePool.getTable(TABLE_B)).thenReturn(tableB);
    }

    /**
     * When receiving an event for a different table than the one specified in the IndexConf, the Indexer should not
     * interact with HBase or Solr.
     */
    @Test
    public void testNonmatchedTable() {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).create();

        Indexer indexer = new Indexer(conf, null, tablePool, solrServer);

        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_B), null, null, null);
        indexer.processEvent(event);

        verifyZeroInteractions(tableA, tableB, solrServer);
    }

    /**
     * When we get a sep event for a row which does not exist anymore (or at least, doesn't contain any relevant
     * columns), then we expect a delete on Solr.
     */
    @Test
    public void testNonExistingRow() throws Exception {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).create();

        when(tableA.get(any(Get.class))).thenReturn(new Result());

        HBaseToSolrMapper mapper = mock(HBaseToSolrMapper.class);
        when(mapper.isRelevantKV(any(KeyValue.class))).thenReturn(true);
        Indexer indexer = new Indexer(conf, mapper, tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        verify(solrServer).deleteById("row1");
        verifyNoMoreInteractions(solrServer);
    }

    /**
     * Create a dummy HBaseToSolrMapper that returns the given value on any calls to containsRequiredData().
     */
    private HBaseToSolrMapper createHbaseToSolrMapper(final boolean containsRequiredDataReturnVal) {
        return new HBaseToSolrMapper() {
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
                return new SolrInputDocument();
            }

            @Override
            public boolean containsRequiredData(Result result) {
                return containsRequiredDataReturnVal;
            }
        };
    }

    @Test
    public void testRowBasedIndexing_RowReadModeNever() throws SolrServerException, IOException {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.NEVER).create();

        HBaseToSolrMapper mapper = createHbaseToSolrMapper(true);

        Indexer indexer = new Indexer(conf, mapper, tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("val")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer).add(arg.capture());
        assertEquals("row1", arg.getValue().getFieldValue("id"));

        verifyZeroInteractions(tableA, tableB);
    }

    @Test
    public void testRowBasedIndexing_RowReadModeDynamic_RereadRequired() throws IOException, SolrServerException {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.DYNAMIC).create();

        HBaseToSolrMapper mapper = createHbaseToSolrMapper(false);

        when(tableA.get(any(Get.class))).thenReturn(new Result(Lists.newArrayList(new KeyValue())));

        Indexer indexer = new Indexer(conf, mapper, tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer).add(arg.capture());
        assertEquals("row1", arg.getValue().getFieldValue("id"));

        // Should have been called twice -- once during the setup, and once during the test itself
        verify(tableA).get(any(Get.class));
    }

    @Test
    public void testRowBasedIndexing_RowReadModeDynamic_NoRereadRequired() throws SolrServerException, IOException {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.DYNAMIC).create();

        HBaseToSolrMapper mapper = createHbaseToSolrMapper(true);

        Indexer indexer = new Indexer(conf, mapper, tablePool, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer).add(arg.capture());
        assertEquals("row1", arg.getValue().getFieldValue("id"));

        verifyZeroInteractions(tableA, tableB);
    }

    @Test
    public void testColumnBasedIndexing() throws Exception {
        IndexConf conf = new IndexConfBuilder().table(TABLE_A).mappingType(IndexConf.MappingType.COLUMN).create();

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

            @Override
            public boolean containsRequiredData(Result result) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Not yet implemented");
            }
        };

        Indexer indexer = new Indexer(conf, mapper, null, solrServer);

        List<KeyValue> kvs = Lists.newArrayList(
                new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"), Bytes.toBytes("msg1"),
                        Bytes.toBytes("the message")), new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"),
                        Bytes.toBytes("msg2"), Bytes.toBytes("another message")));

        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexer.processEvent(event);

        ArgumentCaptor<SolrInputDocument> arg = ArgumentCaptor.forClass(SolrInputDocument.class);
        verify(solrServer, times(2)).add(arg.capture());
        List<SolrInputDocument> docs = arg.getAllValues();
        assertEquals("row1-messages-msg1", docs.get(0).getFieldValue("id"));
        assertEquals("row1-messages-msg2", docs.get(1).getFieldValue("id"));
    }
}
