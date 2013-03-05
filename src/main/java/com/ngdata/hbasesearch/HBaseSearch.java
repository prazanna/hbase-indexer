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
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class HBaseSearch {
    public static void main(String[] args) throws Exception {
        new HBaseSearch().run(args);
    }

    public void run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        if (!sepModel.hasSubscription("index1")) {
            sepModel.addSubscription("index1");
        }

        HttpSolrServer solr = new HttpSolrServer("http://localhost:8983/solr");
        HTablePool tablePool = new HTablePool(conf, 10);

        IndexConf indexConf = new IndexConfBuilder().table("table").create();

        SepConsumer sepConsumer = new SepConsumer("index1", 0, new Indexer(indexConf, tablePool, solr), 10,
                "localhost", zk, conf, null);

        sepConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
