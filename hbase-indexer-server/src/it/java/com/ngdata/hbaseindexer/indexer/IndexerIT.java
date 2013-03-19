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
package com.ngdata.hbaseindexer.indexer;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.Main;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.util.net.NetUtils;
import com.ngdata.hbaseindexer.util.solr.SolrTestingUtility;
import com.ngdata.sep.impl.SepReplicationSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class IndexerIT {
    private static final byte[] TABLE_NAME = Bytes.toBytes("table1");
    private static final byte[] COL_FAMILY = Bytes.toBytes("family1");

    private static Configuration conf;
    private static HBaseTestingUtility hbaseTestUtil;
    private static SolrTestingUtility solrTestingUtility;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // We'll use the same conf object for hbase master/rs, hbase-indexer, and hbase client
        conf = HBaseIndexerConfiguration.create();
        // The following are the standard settings that for hbase regionserver when using the SEP (see SEP docs)
        conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
        conf.setLong("replication.source.sleepforretries", 50);
        conf.set("replication.replicationsource.implementation", SepReplicationSource.class.getName());

        hbaseTestUtil = new HBaseTestingUtility(conf);
        hbaseTestUtil.startMiniZKCluster(1);
        hbaseTestUtil.startMiniCluster(1);

        int zkClientPort = hbaseTestUtil.getZkCluster().getClientPort();

        conf.set("hbaseindexer.zookeeper.connectstring", "localhost:" + zkClientPort);

        solrTestingUtility = new SolrTestingUtility(zkClientPort, NetUtils.getFreePort());
        solrTestingUtility.start();
        solrTestingUtility.uploadConfig("config1",
                Resources.toByteArray(Resources.getResource(IndexerIT.class, "schema.xml")),
                Resources.toByteArray(Resources.getResource(IndexerIT.class, "solrconfig.xml")));
        solrTestingUtility.createCore("collection1_core1", "collection1", "config1", 1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        //  Stop Solr first, as it depends on ZooKeeper
        if (solrTestingUtility != null) {
            solrTestingUtility.stop();
        }

        if (hbaseTestUtil != null) {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }

    @Test
    public void testBasicScenario() throws Exception {
        Main main = new Main();
        main.startServices(conf);

        // Create a table in HBase
        HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
        HColumnDescriptor dataColfamDescriptor = new HColumnDescriptor(COL_FAMILY);
        dataColfamDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
        tableDescriptor.addFamily(dataColfamDescriptor);

        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        hbaseAdmin.createTable(tableDescriptor);
        hbaseAdmin.close();

        // Add an indexer
        WriteableIndexerModel indexerModel = main.getIndexerModel();
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("index1")
                .configuration("<indexer table='table1'><field name='field1_s' value='family1:qualifier1'/></indexer>".getBytes())
                .connectionType("solr")
                .connectionParams(ImmutableMap.of("solr.zk", solrTestingUtility.getZkConnectString(),
                        "solr.collection", "collection1"))
                .build();

        indexerModel.addIndexer(indexerDef);

        // Ingest
        HTable table = new HTable(conf, TABLE_NAME);
        Put put = new Put(Bytes.toBytes("row1"));
        put.add(COL_FAMILY, Bytes.toBytes("qualifier1"), Bytes.toBytes("value1"));
        table.put(put);

        // Commit Solr index and check data is present
        CloudSolrServer solrServer = new CloudSolrServer(solrTestingUtility.getZkConnectString());
        solrServer.setDefaultCollection("collection1");

        long waitUntil = System.currentTimeMillis() + 60000L;
        int resultSize = 0;
        while (resultSize == 0) {
            solrServer.commit();
            QueryResponse response = solrServer.query(new SolrQuery("*:*"));
            resultSize = response.getResults().size();

            if (System.currentTimeMillis() > waitUntil) {
                fail("Document not indexed in Solr within timeout");
            } else {
                System.out.println("Waiting on document to be available in Solr...");
            }
            Thread.sleep(20);
        }

        main.stopServices();
    }
}
