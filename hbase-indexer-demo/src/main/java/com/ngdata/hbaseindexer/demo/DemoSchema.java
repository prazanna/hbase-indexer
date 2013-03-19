/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.hbaseindexer.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Creates the required HBase tables for the HBase Indexer demo.
 */
public class DemoSchema {
    
    public static final String USER_TABLE = "indexdemo-user";
    public static final String MESSAGE_TABLE = "indexdemo-message";
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        createSchema(conf);
    }

    public static void createSchema(Configuration hbaseConf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (!admin.tableExists(USER_TABLE)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(USER_TABLE);

            HColumnDescriptor infoCf = new HColumnDescriptor("info");
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);

            admin.createTable(tableDescriptor);
        }
        
        if (!admin.tableExists(MESSAGE_TABLE)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(MESSAGE_TABLE);
            HColumnDescriptor dataCf = new HColumnDescriptor("content");
            dataCf.setScope(1);
            tableDescriptor.addFamily(dataCf);
            
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }
}
