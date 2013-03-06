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

import com.ngdata.hbasesearch.master.IndexerMaster;
import com.ngdata.hbasesearch.model.api.IndexDefinition;
import com.ngdata.hbasesearch.model.api.WriteableIndexerModel;
import com.ngdata.hbasesearch.model.impl.IndexerModelImpl;
import com.ngdata.hbasesearch.supervisor.IndexerRegistry;
import com.ngdata.hbasesearch.supervisor.IndexerSupervisor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class Main {
    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);

        HTablePool tablePool = new HTablePool(conf, 10);

        WriteableIndexerModel indexModel = new IndexerModelImpl(zk);

        SepModel sepModel = new SepModelImpl(zk, conf);

        IndexerMaster master = new IndexerMaster(zk, indexModel, null, null, conf, "localhost:2181", 30000,
                sepModel, "localhost");
        master.start();

        IndexerRegistry indexerRegistry = new IndexerRegistry();
        IndexerSupervisor supervisor = new IndexerSupervisor(indexModel, zk, "localhost" /* TODO */, indexerRegistry,
                tablePool, conf);

        supervisor.init();

//        IndexDefinition definition = indexModel.newIndex("index1");
//        definition.setConfiguration("<index table='table1'/>".getBytes());
//        indexModel.addIndex(definition);

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
