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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoUserIngester {
    private List<String> names;
    private List<String> domains;
    private AtomicLong totalPuts = new AtomicLong();

    private static final byte[] infoCf = Bytes.toBytes("info");

    // column qualifiers
    private static final byte[] firstNameCq = Bytes.toBytes("firstname");
    private static final byte[] lastNameCq = Bytes.toBytes("lastname");
    private static final byte[] emailCq = Bytes.toBytes("email");
    private static final byte[] ageCq = Bytes.toBytes("age");

    public static void main(String[] args) throws Exception {
        new DemoUserIngester().run(args);
    }

    public void run(String[] args) throws Exception {
        OptionParser parser =  new OptionParser();

        ArgumentAcceptingOptionSpec<Integer> threadsOption =
                parser.accepts("threads", "number of concurrent threads")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(1);

        ArgumentAcceptingOptionSpec<Integer> batchSizeOption =
                parser.accepts("batchsize", "size of multi-puts done to hbase")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(1);

        OptionSet options = parser.parse(args);

        final int threads = threadsOption.value(options);
        final int batchSize = batchSizeOption.value(options);

        Configuration conf = HBaseConfiguration.create();

        DemoSchema.createSchema(conf);

        loadData();

        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(new Putter("thread" + i, conf, batchSize));
            thread.start();
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        System.out.printf("[threads: %s, batch size: %s] Total puts until now %s\n",
                                threads, batchSize, totalPuts.get());
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private class Putter implements Runnable {
        private String name;
        private Configuration conf;
        private int batchSize;

        public Putter(String name, Configuration conf, int batchSize) {
            this.name = name;
            this.conf = conf;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            try {
                HTable htable = new HTable(conf, DemoSchema.USER_TABLE);

                while (true) {
                    List<Put> puts = new ArrayList<Put>();
                    for (int i = 0; i < batchSize; i++) {
                        byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
                        Put put = new Put(rowkey);

                        String firstName = pickName();
                        String lastName = pickName();
                        String email = firstName.toLowerCase() + "@" + pickDomain();
                        int age = (int) Math.ceil(Math.random() * 100);

                        put.add(infoCf, firstNameCq, Bytes.toBytes(firstName));
                        put.add(infoCf, lastNameCq, Bytes.toBytes(lastName));
                        put.add(infoCf, emailCq, Bytes.toBytes(email));
                        put.add(infoCf, ageCq, Bytes.toBytes(age));

                        puts.add(put);
                    }

                    htable.put(puts);
                    totalPuts.addAndGet(puts.size());
                }
            } catch (Throwable t) {
                System.err.println("Thread " + name + " dying because of an error");
                t.printStackTrace(System.err);
            }
        }
    }

    private String pickName() {
        return names.get((int)Math.floor(Math.random() * names.size()));
    }

    private String pickDomain() {
        return domains.get((int)Math.floor(Math.random() * domains.size()));
    }

    private void loadData() throws IOException {
        // Names
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("names/names.txt")));

        names = new ArrayList<String>();

        String line;
        while ((line = reader.readLine()) != null) {
            names.add(line);
        }

        // Domains
        domains = new ArrayList<String>();
        domains.add("gmail.com");
        domains.add("hotmail.com");
        domains.add("yahoo.com");
        domains.add("live.com");
        domains.add("ngdata.com");
    }
}
