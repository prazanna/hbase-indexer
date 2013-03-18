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
package com.ngdata.hbaseindexer.indexer;

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration, and eventually
 * calls Solr.
 */
public abstract class Indexer implements EventListener {

    protected Log log = LogFactory.getLog(getClass());

    private String indexerName;
    protected IndexerConf conf;
    private SolrWriter solrWriter;
    protected ResultToSolrMapper mapper;
    protected UniqueKeyFormatter uniqueKeyFormatter;
    private Predicate<SepEvent> tableEqualityPredicate;
    private final Meter incomingEventsMeter;
    private final Meter applicableEventsMeter;

    /**
     * Instantiate an indexer based on the given {@link IndexerConf}.
     */
    public static Indexer createIndexer(String indexerName, IndexerConf conf, ResultToSolrMapper mapper, HTablePool tablePool,
            SolrServer solrServer) {
        SolrWriter solrWriter = new SolrWriter(indexerName, solrServer);
        switch (conf.getMappingType()) {
        case COLUMN:
            return new ColumnBasedIndexer(indexerName, conf, mapper, solrWriter);
        case ROW:
            return new RowBasedIndexer(indexerName, conf, mapper, tablePool, solrWriter);
        default:
            throw new IllegalStateException("Can't determine the type of indexing to use for mapping type "
                    + conf.getMappingType());
        }
    }

    Indexer(String indexerName, IndexerConf conf, ResultToSolrMapper mapper, SolrWriter solrWriter) {
        this.indexerName = indexerName;
        this.conf = conf;
        this.mapper = mapper;
        try {
            this.uniqueKeyFormatter = conf.getUniqueKeyFormatterClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
        this.solrWriter = solrWriter;

        final byte[] tableNameBytes = Bytes.toBytes(conf.getTable());
        tableEqualityPredicate = new Predicate<SepEvent>() {

            @Override
            public boolean apply(@Nullable SepEvent event) {
                return Arrays.equals(event.getTable(), tableNameBytes);
            }
        };
        
        incomingEventsMeter = Metrics.newMeter(metricName(getClass(), "Incoming events", indexerName),
                "Rate of incoming SEP events", TimeUnit.SECONDS);
        applicableEventsMeter = Metrics.newMeter(metricName(getClass(), "Applicable events", indexerName),
                "Rate of incoming SEP events that are considered applicable", TimeUnit.SECONDS);

    }

    /**
     * Build all new documents and ids to delete based on a list of {@code SepEvent}s.
     * 
     * @param events events that (potentially) trigger index updates
     * @param updateCollector collects updates to be written to Solr
     */
    abstract void calculateIndexUpdates(List<SepEvent> events, SolrUpdateCollector updateCollector) throws IOException;

    @Override
    public void processEvents(List<SepEvent> events) {
        try {

            incomingEventsMeter.mark(events.size());
            SolrUpdateCollector updateCollector = new SolrUpdateCollector(events.size());
            events = Lists.newArrayList(Iterables.filter(events, tableEqualityPredicate));
            applicableEventsMeter.mark(events.size());

            calculateIndexUpdates(events, updateCollector);

            if (!updateCollector.getDocumentsToAdd().isEmpty()) {
                solrWriter.add(updateCollector.getDocumentsToAdd());
            }
            if (!updateCollector.getIdsToDelete().isEmpty()) {
                solrWriter.deleteById(updateCollector.getIdsToDelete());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void stop() {
        IndexerMetricsUtil.shutdownMetrics(getClass(), indexerName);
        IndexerMetricsUtil.shutdownMetrics(mapper.getClass(), indexerName);
    }
    

    static class RowBasedIndexer extends Indexer {
        
        private HTablePool tablePool;
        private Timer rowReadTimer;

        public RowBasedIndexer(String indexerName, IndexerConf conf, ResultToSolrMapper mapper, HTablePool tablePool, SolrWriter solrWriter) {
            super(indexerName, conf, mapper, solrWriter);
            this.tablePool = tablePool;
            rowReadTimer = Metrics.newTimer(metricName(getClass(), "Row read timer", indexerName), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        }

        /**
         * Makes a HBase Result object based on the KeyValue's from the SEP event. Usually, this will only be used in
         * situations where only new data is written (or updates are complete row updates), so we don't expect any
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
            TimerContext timerContext = rowReadTimer.time();
            try {
                HTableInterface table = tablePool.getTable(conf.getTable());
                try {
                    Get get = mapper.getGet(row);
                    return table.get(get);
                } finally {
                    table.close();
                }
            } finally {
                timerContext.stop();
            }
        }

        @Override
        protected void calculateIndexUpdates(List<SepEvent> events, SolrUpdateCollector updateCollector) throws IOException {

            Map<String, SepEvent> idToEvent = calculateUniqueEvents(events);

            for (SepEvent event : idToEvent.values()) {

                Result result = makeResult(event.getKeyValues());
                if (conf.getRowReadMode() == RowReadMode.DYNAMIC) {
                    if (!mapper.containsRequiredData(result)) {
                        result = readRow(event.getRow());
                    }
                }

                boolean rowDeleted = result.isEmpty();

                if (rowDeleted) {
                    // Delete row from Solr as well
                    updateCollector.deleteById(uniqueKeyFormatter.formatRow(event.getRow()));
                    if (log.isDebugEnabled()) {
                        log.debug("Row " + Bytes.toString(event.getRow()) + ": deleted from Solr");
                    }
                } else {
                    SolrInputDocument document = mapper.map(result);
                    document.addField(conf.getUniqueKeyField(), uniqueKeyFormatter.formatRow(event.getRow()));
                    // TODO there should probably some way for the mapper to indicate there was no useful content to
                    // map,  e.g. if there are no fields in the solrWriter document (and should we then perform a delete instead?)
                    updateCollector.add(document);
                    if (log.isDebugEnabled()) {
                        log.debug("Row " + Bytes.toString(event.getRow()) + ": added to Solr");
                    }
                }
            }
        }

        /**
         * Calculate a map of Solr document ids to SepEvents, only taking the most recent event for each document id.
         */
        private Map<String, SepEvent> calculateUniqueEvents(List<SepEvent> events) {
            Map<String, SepEvent> idToEvent = Maps.newHashMap();
            for (SepEvent event : events) {
                // Check if the event contains changes to relevant key values
                boolean relevant = false;
                for (KeyValue kv : event.getKeyValues()) {
                    if (mapper.isRelevantKV(kv)) {
                        relevant = true;
                        break;
                    }
                }

                if (!relevant) {
                    break;
                }
                idToEvent.put(uniqueKeyFormatter.formatRow(event.getRow()), event);
            }
            return idToEvent;
        }

    }

    static class ColumnBasedIndexer extends Indexer {

        public ColumnBasedIndexer(String indexerName, IndexerConf conf, ResultToSolrMapper mapper, SolrWriter solrWriter) {
            super(indexerName, conf, mapper, solrWriter);
        }

        @Override
        protected void calculateIndexUpdates(List<SepEvent> events, SolrUpdateCollector updateCollector) throws IOException {
            Map<String, KeyValue> idToKeyValue = calculateUniqueEvents(events);
            for (Entry<String, KeyValue> idToKvEntry : idToKeyValue.entrySet()) {
                // TODO what to do in case of the various delete types (e.g. delete)
                if (idToKvEntry.getValue().isDeleteType()) {
                    // family?)
                    updateCollector.deleteById(idToKvEntry.getKey());
                } else {
                    Result result = new Result(Collections.singletonList(idToKvEntry.getValue()));
                    SolrInputDocument document = mapper.map(result);
                    document.addField(conf.getUniqueKeyField(), idToKvEntry.getKey());
                    updateCollector.add(document);
                }
            }
        }

        /**
         * Calculate a map of Solr document ids to SepEvents, only taking the most recent event for each document id.
         */
        private Map<String, KeyValue> calculateUniqueEvents(List<SepEvent> events) {
            Map<String, KeyValue> idToKeyValue = Maps.newHashMap();
            for (SepEvent event : events) {
                for (KeyValue kv : event.getKeyValues()) {
                    if (mapper.isRelevantKV(kv)) {
                        String id = uniqueKeyFormatter.formatKeyValue(kv);
                        idToKeyValue.put(id, kv);
                    }
                }
            }
            return idToKeyValue;
        }

    }

}
