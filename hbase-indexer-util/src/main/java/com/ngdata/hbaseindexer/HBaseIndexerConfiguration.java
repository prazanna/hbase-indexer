/*
 * Copyright 2013 NGDATA nv
 *
 * Partly modeled after HBase's HBaseConfiguration, Copyright 2007 The Apache Software Foundation
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

import com.ngdata.hbaseindexer.util.VersionInfo;
import org.apache.hadoop.conf.Configuration;

public class HBaseIndexerConfiguration {
    /**
     * Please use {@link HBaseIndexerConfiguration#create()}.
     */
    private HBaseIndexerConfiguration() {
    }

    /**
     * Creates a Configuration with HBase Indexer resources
     */
    public static Configuration create() {
        Configuration conf = new Configuration();
        return addHbaseIndexerResources(conf);
    }

    public static Configuration addHbaseIndexerResources(Configuration conf) {
        conf.addResource("hbase-default.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("hbase-indexer-default.xml");
        conf.addResource("hbase-indexer-site.xml");

        checkDefaultsVersion(conf);
        return conf;
    }

    private static void checkDefaultsVersion(Configuration conf) {
        if (conf.getBoolean("hbaseindexer.defaults.for.version.skip", Boolean.FALSE)) return;
        String defaultsVersion = conf.get("hbaseindexer.defaults.for.version");
        String thisVersion = VersionInfo.getVersion();
        if (!thisVersion.equals(defaultsVersion)) {
            throw new RuntimeException(
                    "hbase-indexer-default.xml file seems to be for and old version of HBase Indexer (" +
                            defaultsVersion + "), this version is " + thisVersion);
        }
    }

    /**
     * For debugging. Dump configurations to system output as xml format.
     */
    public static void main(String[] args) throws Exception {
        HBaseIndexerConfiguration.create().writeXml(System.out);
    }
}
