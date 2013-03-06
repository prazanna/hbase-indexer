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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.ngdata.hbasesearch.conf.IndexConf;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration,
 * and eventually calls Solr.
 */
public class Indexer implements EventListener {
    private IndexConf conf;
    private SolrServer solr;
    private HTablePool tablePool;
    private HBaseToSolrMapper mapper;
    private UniqueKeyFormatter uniqueKeyFormatter;

    public Indexer(IndexConf conf, HBaseToSolrMapper mapper, HTablePool tablePool, SolrServer solrServer) {
        this.conf = conf;
        this.mapper = mapper;
        try {
            this.uniqueKeyFormatter = conf.getUniqueKeyFormatterClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
        this.tablePool = tablePool;
        this.solr = solrServer;
    }

    @Override
    public void processEvent(SepEvent event) {
        try {
            if (!Arrays.equals(event.getTable(), Bytes.toBytes(conf.getTable()))) {
                return;
            }

            switch (conf.getMappingType()) {
                case ROW:
                    performRowBasedMapping(event);
                    break;
                case COLUMN:
                    performColumnBasedMapping(event);
                    break;
                default:
                    throw new RuntimeException("Unexpected mapping type: " + conf.getMappingType());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void performRowBasedMapping(SepEvent event) throws IOException, SolrServerException {
        // Check if the event contains changes to relevant key values
        boolean relevant = false;
        for (KeyValue kv : event.getKeyValues()) {
            if (mapper.isRelevantKV(kv)) {
                relevant = true;
                break;
            }
        }

        if (!relevant) {
            return;
        }

        Result result;
        switch (conf.getRowReadMode()) {
            case ALWAYS:
                result = readRow(event.getRow());
                break;
            case NEVER:
                result = makeResult(event.getKeyValues());
                break;
            default:
                throw new RuntimeException("Unexpected row read mode: " + conf.getRowReadMode());
        }

        boolean rowDeleted = result.isEmpty();

        if (rowDeleted) {
            // Delete row from Solr as well
            solr.deleteById(uniqueKeyFormatter.format(event.getRow()));
            System.out.println("Row " + Bytes.toString(event.getRow()) + ": deleted from Solr");
        } else {
            SolrInputDocument document = mapper.map(result);
            document.addField(conf.getUniqueKeyField(), uniqueKeyFormatter.format(event.getRow()));
            // TODO there should probably some way for the mapper to indicate there was no useful content to map,
            // e.g. if there are no fields in the solr document (and should we then perform a delete instead?)
            solr.add(document);
            System.out.println("Row " + Bytes.toString(event.getRow()) + ": added to Solr");
        }
    }

    private void performColumnBasedMapping(SepEvent event) throws IOException, SolrServerException {
        for (KeyValue kv : event.getKeyValues()) {
            if (mapper.isRelevantKV(kv)) {
                String id = uniqueKeyFormatter.format(kv.getRow(), kv.getFamily(), kv.getQualifier());
                if (kv.isDeleteType()) { // TODO what to do in case of the various delete types (e.g. delete family?)
                    solr.deleteById(id);
                } else {
                    Result result = new Result(Lists.newArrayList(kv));
                    SolrInputDocument document = mapper.map(result);
                    document.addField(conf.getUniqueKeyField(), id);
                    solr.add(document);
                }
            }
        }
    }

    /**
     * Makes a HBase Result object based on the KeyValue's from the SEP event. Usually, this will only be used
     * in situations where only new data is written (or updates are complete row updates), so we don't expect any
     * delete-type key-values, but just to be sure we filter them out.
     */
    private Result makeResult(List<KeyValue> eventKeyValues) {
        List<KeyValue> keyValues = new ArrayList<KeyValue>(eventKeyValues.size());

        for (KeyValue kv : eventKeyValues) {
            if (!kv.isDelete() && !kv.isInternal()) {
                keyValues.add(kv);
            }
        }

        return new Result(keyValues);
    }

    private Result readRow(byte[] row) throws IOException {
        HTableInterface table = tablePool.getTable(conf.getTable());
        try {
            Get get = mapper.getGet(row);
            return table.get(get);
        } finally {
            table.close();
        }
    }
}
