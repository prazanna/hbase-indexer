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
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;

import java.util.Arrays;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration,
 * and eventually calls Solr.
 */
public class Indexer implements EventListener {
    private IndexConf conf;
    private SolrServer solr;
    private HTablePool tablePool;

    public Indexer(IndexConf conf, HTablePool tablePool, SolrServer solrServer) {
        this.conf = conf;
        this.tablePool = tablePool;
        this.solr = solrServer;
    }

    @Override
    public void processEvent(SepEvent event) {
        try {
            if (!Arrays.equals(event.getTable(), Bytes.toBytes(conf.getTable()))) {
                return;
            }

            HTableInterface table = tablePool.getTable(event.getTable());
            Result result;
            boolean rowDeleted = false;
            try {
                result = table.get(new Get(event.getRow()));
                if (result.isEmpty()) {
                    rowDeleted = true;
                }
            } finally {
                table.close();
            }

            if (rowDeleted) {
                // Delete row from Solr as well
                solr.deleteById(Bytes.toString(event.getRow()));
                System.out.println("Row " + Bytes.toString(event.getRow()) + ": deleted from Solr");
            } else {
                // TODO
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
