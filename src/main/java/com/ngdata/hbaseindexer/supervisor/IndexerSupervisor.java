package com.ngdata.hbaseindexer.supervisor;


import com.google.common.base.Objects;
import com.ngdata.hbaseindexer.HBaseToSolrMapper;
import com.ngdata.hbaseindexer.Indexer;
import com.ngdata.hbaseindexer.ResultToSolrMapper;
import com.ngdata.hbaseindexer.conf.IndexConf;
import com.ngdata.hbaseindexer.conf.XmlIndexConfReader;
import com.ngdata.hbaseindexer.model.api.IndexDefinition;
import com.ngdata.hbaseindexer.model.api.IndexNotFoundException;
import com.ngdata.hbaseindexer.model.api.IndexUpdateState;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import com.ngdata.hbaseindexer.model.api.IndexerModelEvent;
import com.ngdata.hbaseindexer.model.api.IndexerModelListener;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.zookeeper.KeeperException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.*;

/**
 * Responsible for starting, stopping and restarting {@link Indexer}s for the indexes defined in the
 * {@link IndexModel}.
 */
public class IndexerSupervisor {
    private final IndexerModel indexerModel;

    private final ZooKeeperItf zk;

    private final String hostName;

    private final IndexerModelListener listener = new MyListener();

    private final Map<String, IndexUpdaterHandle> indexUpdaters = new HashMap<String, IndexUpdaterHandle>();

    private final Object indexUpdatersLock = new Object();

    private final BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private EventWorker eventWorker;

    private Thread eventWorkerThread;

    private HttpClient httpClient;

    private ThreadSafeClientConnManager connectionManager;

    private final IndexerRegistry indexerRegistry;

    private final HTablePool htablePool;

    private final Configuration hbaseConf;

    private final Log log = LogFactory.getLog(getClass());

    public IndexerSupervisor(IndexerModel indexerModel, ZooKeeperItf zk, String hostName,
            IndexerRegistry indexerRegistry, HTablePool htablePool, Configuration hbaseConf)
            throws IOException, InterruptedException {
        this.indexerModel = indexerModel;
        this.zk = zk;
        this.hostName = hostName;
        this.indexerRegistry = indexerRegistry;
        this.htablePool = htablePool;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void init() {
        connectionManager = new ThreadSafeClientConnManager();
        httpClient = new DefaultHttpClient(connectionManager);

        eventWorker = new EventWorker();
        eventWorkerThread = new Thread(eventWorker, "IndexerWorkerEventWorker");
        eventWorkerThread.start();

        synchronized (indexUpdatersLock) {
            Collection<IndexDefinition> indexes = indexerModel.getIndexes(listener);

            for (IndexDefinition index : indexes) {
                if (shouldRunIndexUpdater(index)) {
                    addIndexUpdater(index);
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        eventWorker.stop();
        eventWorkerThread.interrupt();
        try {
            eventWorkerThread.join();
        } catch (InterruptedException e) {
            log.info("Interrupted while joining eventWorkerThread.");
        }

        for (IndexUpdaterHandle handle : indexUpdaters.values()) {
            try {
                handle.stop();
            } catch (InterruptedException e) {
                // Continue the stop procedure
            }
        }

        connectionManager.shutdown();
    }

    private void addIndexUpdater(IndexDefinition index) {
        IndexUpdaterHandle handle = null;
        try {
            IndexConf indexConf = new XmlIndexConfReader().read(new ByteArrayInputStream(index.getConfiguration()));

            // TODO need something real here
            HttpSolrServer solr = new HttpSolrServer("http://localhost:8983/solr", httpClient);

            // create and register the indexer
            HBaseToSolrMapper mapper = new ResultToSolrMapper(
                    indexConf.getFieldDefinitions(), indexConf.getDocumentExtractDefinitions());
            Indexer indexer = new Indexer(indexConf, mapper, htablePool, solr);
            indexerRegistry.register(index.getName(), indexer);

            SepConsumer sepConsumer = new SepConsumer(index.getQueueSubscriptionId(),
                    index.getSubscriptionTimestamp(), indexer, 10 /* TODO make configurable */, hostName,
                    zk, hbaseConf, null);
            handle = new IndexUpdaterHandle(index, sepConsumer);
            handle.start();

            indexUpdaters.put(index.getName(), handle);

            log.info("Started index updater for index " + index.getName());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            log.error("Problem starting index updater for index " + index.getName(), t);

            if (handle != null) {
                // stop any listeners that might have been started
                try {
                    handle.stop();
                } catch (Throwable t2) {
                    if (t2 instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("Problem stopping listeners for failed-to-start index updater for index '" +
                            index.getName() + "'", t2);
                }
            }
        }
    }

    private void updateIndexUpdater(IndexDefinition index) {
        IndexUpdaterHandle handle = indexUpdaters.get(index.getName());

        if (handle.indexDef.getZkDataVersion() >= index.getZkDataVersion()) {
            return;
        }

        boolean relevantChanges = !Arrays.equals(handle.indexDef.getConfiguration(), index.getConfiguration()) ||
                !handle.indexDef.getSolrShards().equals(index.getSolrShards()) ||
                !Objects.equal(handle.indexDef.getShardingConfiguration(), index.getShardingConfiguration()) ||
                handle.indexDef.isEnableDerefMap() != index.isEnableDerefMap();

        if (!relevantChanges) {
            return;
        }

        if (removeIndexUpdater(index.getName())) {
            addIndexUpdater(index);
        }
    }

    private boolean removeIndexUpdater(String indexName) {
        indexerRegistry.unregister(indexName);

        IndexUpdaterHandle handle = indexUpdaters.get(indexName);

        if (handle == null) {
            return true;
        }

        try {
            handle.stop();
            indexUpdaters.remove(indexName);
            log.info("Stopped indexer updater for index " + indexName);
            return true;
        } catch (Throwable t) {
            log.fatal("Failed to stop an IndexUpdater that should be stopped.", t);
            return false;
        }
    }

    private class MyListener implements IndexerModelListener {
        @Override
        public void process(IndexerModelEvent event) {
            try {
                // Because the actions we take in response to events might take some time, we
                // let the events process by another thread, so that other watchers do not
                // have to wait too long.
                eventQueue.put(event);
            } catch (InterruptedException e) {
                log.info("IndexerWorker.IndexerModelListener interrupted.");
            }
        }
    }

    private boolean shouldRunIndexUpdater(IndexDefinition index) {
        return index.getUpdateState() == IndexUpdateState.SUBSCRIBE_AND_LISTEN &&
                index.getQueueSubscriptionId() != null &&
                !index.getGeneralState().isDeleteState();
    }

    private class IndexUpdaterHandle {
        private final IndexDefinition indexDef;
        private final SepConsumer sepConsumer;

        public IndexUpdaterHandle(IndexDefinition indexDef, SepConsumer sepEventSlave) {
            this.indexDef = indexDef;
            this.sepConsumer = sepEventSlave;
        }

        public void start() throws InterruptedException, KeeperException, IOException {
            sepConsumer.start();
        }

        public void stop() throws InterruptedException {
            Closer.close(sepConsumer);
        }
    }

    private class EventWorker implements Runnable {
        private volatile boolean stop = false;

        public void stop() {
            stop = true;
        }

        @Override
        public void run() {
            while (!stop) { // We need the stop flag because some code (HBase client code) eats interrupted flags
                if (Thread.interrupted()) {
                    return;
                }

                try {
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    IndexerModelEvent event = eventQueue.take();
                    if (event.getType() == INDEX_ADDED || event.getType() == INDEX_UPDATED) {
                        try {
                            IndexDefinition index = indexerModel.getIndex(event.getIndexName());
                            if (shouldRunIndexUpdater(index)) {
                                if (indexUpdaters.containsKey(index.getName())) {
                                    updateIndexUpdater(index);
                                } else {
                                    addIndexUpdater(index);
                                }
                            } else {
                                removeIndexUpdater(index.getName());
                            }
                        } catch (IndexNotFoundException e) {
                            removeIndexUpdater(event.getIndexName());
                        } catch (Throwable t) {
                            log.error("Error in IndexerWorker's IndexerModelListener.", t);
                        }
                    } else if (event.getType() == INDEX_REMOVED) {
                        removeIndexUpdater(event.getIndexName());
                    }
                } catch (InterruptedException e) {
                    log.info("IndexerWorker.EventWorker interrupted.");
                    return;
                } catch (Throwable t) {
                    log.error("Error processing indexer model event in IndexerWorker.", t);
                }
            }
        }
    }
}
