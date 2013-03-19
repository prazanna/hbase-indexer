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
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoMessageIngester {

    private List<String> words;
    private Random random;

    public static void main(String[] args) throws IOException {
        new DemoMessageIngester().run();
    }

    public DemoMessageIngester() {
        this.words = loadWords();
        random = new Random();
    }

    public void run() throws IOException {
        
        Configuration conf = HBaseConfiguration.create();

        DemoSchema.createSchema(conf);
        
        final byte[] contentCf = Bytes.toBytes("content");
        

        HTable htable = new HTable(conf, DemoSchema.MESSAGE_TABLE);

        while (true) {
            int userId = random.nextInt(1000);
            byte[] rowkey = Bytes.toBytes(String.valueOf(userId));
            Put put = new Put(rowkey);
            
            long timestamp = Long.MAX_VALUE - System.currentTimeMillis();
            int nonce = random.nextInt();

            byte[] qualifier = Bytes.add(Bytes.toBytes(timestamp), Bytes.toBytes("_"), Bytes.toBytes(nonce));
            put.add(contentCf, qualifier, Bytes.toBytes(createMessageText()));
            htable.put(put);
            
            System.out.printf("Added message for user %d with timestamp %d\n", userId, timestamp);
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
