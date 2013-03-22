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
package com.ngdata.hbaseindexer.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoMessageIngester {

    private List<String> words;
    private Random random;
    private AtomicLong totalPuts = new AtomicLong();

    private static final byte[] contentCf = Bytes.toBytes("content");

    public static void main(String[] args) throws IOException {
        new DemoMessageIngester().run(args);
    }

    public DemoMessageIngester() {
        this.words = loadWords();
        random = new Random();
    }

    public void run(String[] args) throws IOException {
        
        OptionParser parser =  new OptionParser();

        parser.accepts("h", "help");

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

        ArgumentAcceptingOptionSpec<Integer> userCountOption =
                parser.accepts("users", "number of different users to generate messages for")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(1000);

        OptionSet options = parser.parse(args);

        if (options.has("h")) {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        final int threads = threadsOption.value(options);
        final int batchSize = batchSizeOption.value(options);
        final int userCount = userCountOption.value(options);

        Configuration conf = HBaseIndexerConfiguration.create();

        DemoSchema.createSchema(conf);


        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(new Putter("thread" + i, conf, batchSize, userCount));
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
        private int userCount;

        public Putter(String name, Configuration conf, int batchSize, int userCount) {
            this.name = name;
            this.conf = conf;
            this.batchSize = batchSize;
            this.userCount = userCount;
        }

        @Override
        public void run() {
            try {
                HTable htable = new HTable(conf, DemoSchema.MESSAGE_TABLE);

                while (true) {
                    List<Put> puts = new ArrayList<Put>();
                    for (int i = 0; i < batchSize; i++) {
                        int userId = random.nextInt(userCount);
                        byte[] rowkey = Bytes.toBytes(String.valueOf(userId));
                        Put put = new Put(rowkey);

                        long timestamp = Long.MAX_VALUE - System.currentTimeMillis();
                        int nonce = random.nextInt();

                        byte[] qualifier = Bytes.add(Bytes.toBytes(timestamp), Bytes.toBytes("_"), Bytes.toBytes(nonce));
                        put.add(contentCf, qualifier, Bytes.toBytes(createMessageText()));
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

    private String createMessageText() {
        int numWords = 100 + random.nextInt(1000);
        List<String> wordSubset = Lists.newArrayListWithCapacity(numWords);
        for (int i = 0; i < numWords; i++) {
            wordSubset.add(words.get(random.nextInt(words.size())));
        }
        return Joiner.on(' ').join(wordSubset);
    }

    private static List<String> loadWords() {
        try {
            ZipInputStream zipInputStream = new ZipInputStream(
                    DemoMessageIngester.class.getResourceAsStream("words/ispell-enwl-3.1.20.zip"));
            ZipEntry entry;
            List<String> words = Lists.newArrayList();
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if ("README".equals(entry.getName())) {
                    zipInputStream.closeEntry();
                    continue;
                }

                BufferedReader reader = new BufferedReader(new InputStreamReader(zipInputStream));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        words.add(line);
                    }
                }
                zipInputStream.closeEntry();
            }

            return words;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

}
