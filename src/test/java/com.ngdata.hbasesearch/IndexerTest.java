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

import com.ngdata.hbasesearch.conf.IndexConf;
import com.ngdata.hbasesearch.conf.IndexConfBuilder;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.junit.Test;

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
        Indexer indexer = new Indexer(conf, tablePool, solrServer);

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

        SolrServer solrServer = mock(SolrServer.class);

        Indexer indexer = new Indexer(conf, tablePool, solrServer);

        SepEvent event = new SepEvent(Bytes.toBytes("table1"), Bytes.toBytes("row1"), null, null);
        indexer.processEvent(event);

        verify(solrServer).deleteById("row1");
        verifyNoMoreInteractions(solrServer);
    }
}
