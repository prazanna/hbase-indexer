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

import com.ngdata.hbaseindexer.master.IndexerMaster;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.supervisor.IndexerRegistry;
import com.ngdata.hbaseindexer.supervisor.IndexerSupervisor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;

public class Main {
    private Log log = LogFactory.getLog(getClass());

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        // The same configuration object is used for both hbase-indexer as for hbase client access
        Configuration conf = HBaseIndexerConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
                conf.get("hbase.regionserver.dns.interface", "default"),
                conf.get("hbase.regionserver.dns.nameserver", "default")));

        log.debug("Using hostname " + hostname);

        String zkConnectString = conf.get("hbaseindexer.zookeeper.connectstring");
        int zkSessionTimeout = conf.getInt("hbaseindexer.zookeeper.session.timeout", 30000);
        ZooKeeperItf zk = ZkUtil.connect(zkConnectString, zkSessionTimeout);

        HTablePool tablePool = new HTablePool(conf, 10 /* TODO configurable */);

        WriteableIndexerModel indexModel = new IndexerModelImpl(zk);

        SepModel sepModel = new SepModelImpl(zk, conf);

        IndexerMaster master = new IndexerMaster(zk, indexModel, null, null, conf, zkConnectString, zkSessionTimeout,
                sepModel, hostname);
        master.start();

        IndexerRegistry indexerRegistry = new IndexerRegistry();
        IndexerSupervisor supervisor = new IndexerSupervisor(indexModel, zk, hostname, indexerRegistry,
                tablePool, conf);

        supervisor.init();
      
//        IndexDefinition definition = indexModel.newIndex("index6");
//        definition.setConfiguration("<index table='sep-user-demo'><field name='features' value='info:*' source='value' type='string'/></index>".getBytes());
//        indexModel.addIndex(definition);

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
