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
package com.ngdata.hbaseindexer.model.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import com.ngdata.hbaseindexer.model.api.IndexerConcurrentModificationException;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerExistsException;
import com.ngdata.hbaseindexer.model.api.IndexerModelException;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.model.api.IndexerUpdateException;
import com.ngdata.hbaseindexer.model.api.IndexerValidityException;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.sep.util.zookeeper.ZooKeeperOperation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerModelEvent;
import com.ngdata.hbaseindexer.model.api.IndexerModelListener;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.util.zookeeper.ZkLock;
import com.ngdata.hbaseindexer.util.zookeeper.ZkLockException;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;
import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.*;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;

// About how the indexer conf is stored in ZooKeeper
// -------------------------------------------------
// I had to make the decision of whether to store all properties of an index in the
// data of one node, or rather to add these properties as subnodes.
//
// The advantages of putting index properties in subnodes are:
//  - they allow to watch inidividual properties, so you know which one changed
//  - each property can be updated individually
//  - there is no impact of big properties, like the indexerconf XML, on other
//    ones
//
// The advantages of putting all index properties in the data of one znode are:
//  - the atomic create or update of an index is easy/possible. ZK does not have
//    transactions. Intermediate state while performing updates is not visible.
//  - watching is simpler: just watch one node, rather than having to register
//    watches on every child node and on the parent for children changes.
//  - in practice it is usually still easy to know what individual properties
//    changed, by comparing with previous state you hold yourself.
//
// So the clear winner was to put all properties in the data of one znode.
// It is much easier: less work, reduced complexity, less chance for errors.
// Also, the state of indexes does not change frequently, so that the data
// of this znode is somewhat bigger is not really important.


/**
 * Implementation of IndexerModel.
 *
 * <p>Usage: typically in an application you will create just one instance of IndexerModel and share it.
 * This is a relatively heavy object which runs threads. IMPORTANT: when done using it, call the stop()
 * method to properly shut down everything.</p>
 *
 */
public class IndexerModelImpl implements WriteableIndexerModel {
    private final ZooKeeperItf zk;

    /**
     * Cache of the indexers as they are stored in ZK. Updated based on ZK watcher events. People who update
     * this cache should synchronize on {@link #indexers_lock}.
     *
     * <p>We don't really need a cache for performance reasons. It does however allow to figure out what
     * indexers have been added or removed when receiving a children-changed ZK event. It also makes that
     * someone who reads IndexerDefinitions doesn't have to worry about ZK connection issues.</p>
     */
    private final Map<String, IndexerDefinition> indexers = new ConcurrentHashMap<String, IndexerDefinition>(16, 0.75f, 1);

    /**
     * Lock that should be obtained when making changes to {@link #indexers}.
     */
    private final Object indexers_lock = new Object();

    private final Set<IndexerModelListener> listeners = Collections.newSetFromMap(new IdentityHashMap<IndexerModelListener, Boolean>());

    private final Watcher watcher = new IndexModelChangeWatcher();

    private final Watcher connectStateWatcher = new ConnectStateWatcher();

    private final IndexerCacheRefresher indexerCacheRefresher = new IndexerCacheRefresher();

    private boolean stopped = false;

    private final Log log = LogFactory.getLog(getClass());

    private final String indexerCollectionPath;

    private final String indexerTrashPath;

    private final String indexerCollectionPathSlash;

    public IndexerModelImpl(ZooKeeperItf zk, String zkRoot) throws InterruptedException, KeeperException {
        this.zk = zk;

        this.indexerCollectionPath = zkRoot + "/indexer";
        this.indexerCollectionPathSlash = indexerCollectionPath + "/";
        this.indexerTrashPath = zkRoot + "/indexer-trash";

        ZkUtil.createPath(zk, indexerCollectionPath);
        ZkUtil.createPath(zk, indexerTrashPath);

        zk.addDefaultWatcher(connectStateWatcher);

        indexerCacheRefresher.start();
        indexerCacheRefresher.waitUntilStarted();
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        stopped = true;
        zk.removeDefaultWatcher(connectStateWatcher);
        indexerCacheRefresher.shutdown();
    }

    @Override
    public void addIndexer(IndexerDefinition indexer) throws IndexerExistsException, IndexerModelException, IndexerValidityException {
        assertValid(indexer);

        if (indexer.getIncrementalIndexingState() != IndexerDefinition.IncrementalIndexingState.DO_NOT_SUBSCRIBE) {
            indexer = new IndexerDefinitionBuilder().startFrom(indexer).subscriptionTimestamp(System.currentTimeMillis()).build();
        }
        final String indexerPath = indexerCollectionPath + "/" + indexer.getName();
        final byte[] data = IndexerDefinitionJsonSerDeser.INSTANCE.toJsonBytes(indexer);

        try {
            zk.retryOperation(new ZooKeeperOperation<String>() {
                @Override
                public String execute() throws KeeperException, InterruptedException {
                    return zk.create(indexerPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            });
        } catch (KeeperException.NodeExistsException e) {
            throw new IndexerExistsException(indexer.getName());
        } catch (Exception e) {
            throw new IndexerModelException("Error creating indexer.", e);
        }
  
    }

    private void assertValid(IndexerDefinition indexer) throws IndexerValidityException {
        if (indexer.getName() == null || indexer.getName().length() == 0)
            throw new IndexerValidityException("Name should not be null or zero-length");

        if (indexer.getConfiguration() == null)
            throw new IndexerValidityException("Configuration should not be null.");

        if (indexer.getLifecycleState() == null)
            throw new IndexerValidityException("General state should not be null.");

        if (indexer.getBatchIndexingState() == null)
            throw new IndexerValidityException("Build state should not be null.");

        if (indexer.getIncrementalIndexingState() == null)
            throw new IndexerValidityException("Update state should not be null.");

        if (indexer.getActiveBatchBuildInfo() != null) {
            ActiveBatchBuildInfo info = indexer.getActiveBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexerValidityException("Job id of active batch build cannot be null.");
        }


        // TODO FIXME disabled this code (after copying from Lily)
//       boolean hasShards = index.getSolrShards() != null && !index.getSolrShards().isEmpty();
//       boolean hasCollection = index.getSolrCollection() != null;
//       boolean hasZkConnectionString = index.getZkConnectionString() != null && !index.getZkConnectionString().isEmpty();
//
//       if (hasCollection && hasShards) {
//           throw new IndexValidityException("Ambiguous solr configuration in index defintion. Setting a solr " +
//                   "collection together with solr shards has no use. Set either the solr shards or collection.");
//       }
//
//       if (hasShards && hasZkConnectionString) {
//           throw new IndexValidityException("Ambiguous solr configuration in index defintion. Setting a solr " +
//                   "zookeeper connection together with solr shards has no use. Set either the solr shards or " +
//                   "zookeeper connection.");
//       }
//       if (!hasShards && !hasZkConnectionString) {
//           throw new IndexValidityException("Incomplete solr configuration in index defintion. You need at least " +
//                   "a shard or a zookeeper connection string.");
//       }

        if (indexer.getLastBatchBuildInfo() != null) {
            BatchBuildInfo info = indexer.getLastBatchBuildInfo();
            if (info.getJobId() == null)
                throw new IndexerValidityException("Job id of last batch build cannot be null.");
            if (info.getJobState() == null)
                throw new IndexerValidityException("Job state of last batch build cannot be null.");
        }

//        for (String shard : index.getSolrShards().values()) {
//            try {
//                URI uri = new URI(shard);
//                if (!uri.isAbsolute()) {
//                    throw new IndexValidityException("Solr shard URI is not absolute: " + shard);
//                }
//            } catch (URISyntaxException e) {
//                throw new IndexValidityException("Invalid Solr shard URI: " + shard);
//            }
//        }

        // TODO FIXME disabled this code (after copying from Lily)
//        if (index.getShardingConfiguration() != null) {
//            // parse it + check used shards -> requires dependency on the engine or moving the relevant classes
//            // to the model
//            ShardSelector selector;
//            try {
//                selector = JsonShardSelectorBuilder.build(index.getShardingConfiguration());
//            } catch (ShardingConfigException e) {
//                throw new IndexValidityException("Error with sharding configuration.", e);
//            }
//
//            Set<String> shardNames = index.getSolrShards().keySet();
//
//            for (String shard : selector.getShards()) {
//                if (!shardNames.contains(shard)) {
//                    throw new IndexValidityException("The sharding configuration refers to a shard that is not" +
//                    " in the set of available shards. Shard: " + shard);
//                }
//            }
//        }
//
//        try {
//            IndexerConfBuilder.validate(new ByteArrayInputStream(index.getConfiguration()));
//        } catch (IndexerConfException e) {
//            throw new IndexValidityException("The indexer configuration is not XML well-formed or valid.", e);
//        }
//
//        if (index.getBatchIndexConfiguration() != null && index.getBatchBuildState() !=
//                IndexBatchBuildState.BUILD_REQUESTED) {
//            throw new IndexValidityException("The build state must be set to BUILD_REQUESTED when setting a batchIndexConfiguration");
//        }
    }

    @Override
    public void updateIndexerInternal(final IndexerDefinition indexer) throws InterruptedException, KeeperException,
            IndexerNotFoundException, IndexerConcurrentModificationException, IndexerValidityException {

        assertValid(indexer);

        final byte[] newData = IndexerDefinitionJsonSerDeser.INSTANCE.toJsonBytes(indexer);

        try {
            zk.retryOperation(new ZooKeeperOperation<Stat>() {
                @Override
                public Stat execute() throws KeeperException, InterruptedException {
                    return zk.setData(indexerCollectionPathSlash + indexer.getName(), newData, indexer.getOccVersion());
                }
            });
        } catch (KeeperException.NoNodeException e) {
            throw new IndexerNotFoundException(indexer.getName());
        } catch (KeeperException.BadVersionException e) {
            throw new IndexerConcurrentModificationException(indexer.getName());
        }
    }

    @Override
    public void updateIndexer(final IndexerDefinition indexer, String lock) throws InterruptedException, KeeperException,
            IndexerNotFoundException, IndexerConcurrentModificationException, ZkLockException, IndexerUpdateException,
            IndexerValidityException {

        if (!ZkLock.ownsLock(zk, lock)) {
            throw new IndexerUpdateException("You are not owner of the indexer's lock, the lock path is: " + lock);
        }

        assertValid(indexer);

        IndexerDefinition currentIndexer = getFreshIndexer(indexer.getName());

        if (currentIndexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED ||
                currentIndexer.getLifecycleState() == LifecycleState.DELETING) {
            throw new IndexerUpdateException("An indexer in state " + indexer.getLifecycleState() + " cannot be modified.");
        }

        if (indexer.getBatchIndexingState() == BatchIndexingState.BUILD_REQUESTED &&
                currentIndexer.getBatchIndexingState() != BatchIndexingState.INACTIVE &&
                currentIndexer.getBatchIndexingState() != BatchIndexingState.BUILD_REQUESTED) {
            throw new IndexerUpdateException("Cannot move batch indexing state from " + currentIndexer.getBatchIndexingState() +
                    " to " + indexer.getBatchIndexingState());
        }

        if (currentIndexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED) {
            throw new IndexerUpdateException("An indexer in the state " + LifecycleState.DELETE_REQUESTED +
                    " cannot be updated.");
        }

        if (!Objects.equal(currentIndexer.getActiveBatchBuildInfo(), indexer.getActiveBatchBuildInfo())) {
            throw new IndexerUpdateException("The active batch build info cannot be modified by users.");
        }

        if (!Objects.equal(currentIndexer.getLastBatchBuildInfo(), indexer.getLastBatchBuildInfo())) {
            throw new IndexerUpdateException("The last batch build info cannot be modified by users.");
        }

        updateIndexerInternal(indexer);

    }

    @Override
    public void deleteIndexerInternal(final String indexerName) throws IndexerModelException {
        final String indexerPath = indexerCollectionPathSlash + indexerName;
        final String indexerLockPath = indexerPath + "/lock";

        try {
            // Make a copy of the index data in the index trash
            zk.retryOperation(new ZooKeeperOperation<Object>() {
                @Override
                public Object execute() throws KeeperException, InterruptedException {
                    byte[] data = zk.getData(indexerPath, false, null);

                    String trashPath = indexerTrashPath + "/" + indexerName;

                    // An indexer with the same name might have existed before and hence already exist
                    // in the indexer trash, handle this by appending a sequence number until a unique
                    // name is found.
                    String baseTrashpath = trashPath;
                    int count = 0;
                    while (zk.exists(trashPath, false) != null) {
                        count++;
                        trashPath = baseTrashpath + "." + count;
                    }

                    zk.create(trashPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    return null;
                }
            });

            // The loop below is normally not necessary, since we disallow taking new locks on indexers
            // which are being deleted.
            int tryCount = 0;
            while (true) {
                boolean success = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                    @Override
                    public Boolean execute() throws KeeperException, InterruptedException {
                        try {
                            // Delete the indexer lock if it exists
                            if (zk.exists(indexerLockPath, false) != null) {
                                List<String> children = Collections.emptyList();
                                try {
                                    children = zk.getChildren(indexerLockPath, false);
                                } catch (KeeperException.NoNodeException e) {
                                    // ok
                                }

                                for (String child : children) {
                                    try {
                                        zk.delete(indexerLockPath + "/" + child, -1);
                                    } catch (KeeperException.NoNodeException e) {
                                        // ignore, node was already removed
                                    }
                                }

                                try {
                                    zk.delete(indexerLockPath, -1);
                                } catch (KeeperException.NoNodeException e) {
                                    // ignore
                                }
                            }

                            zk.delete(indexerPath, -1);

                            return true;
                        } catch (KeeperException.NotEmptyException e) {
                            // Someone again took a lock on the indexer, retry
                        }
                        return false;
                    }
                });

                if (success)
                    break;

                tryCount++;
                if (tryCount > 10) {
                    throw new IndexerModelException("Failed to delete indexer because it still has child data. Indexer: "
                            + indexerName);
                }
            }
        } catch (Throwable t) {
            if (t instanceof InterruptedException)
                Thread.currentThread().interrupt();
            throw new IndexerModelException("Failed to delete indexer " + indexerName, t);
        }
    }

    @Override
    public String lockIndexerInternal(String indexerName, boolean checkDeleted) throws ZkLockException,
            IndexerNotFoundException, InterruptedException, KeeperException, IndexerModelException {

        IndexerDefinition indexer = getFreshIndexer(indexerName);

        if (checkDeleted) {
            if (indexer.getLifecycleState() == LifecycleState.DELETE_REQUESTED ||
                    indexer.getLifecycleState() == LifecycleState.DELETING) {
                throw new IndexerModelException("An indexer in state " + indexer.getLifecycleState() + " cannot be locked.");
            }
        }

        final String lockPath = indexerCollectionPathSlash + indexerName + "/lock";

        //
        // Create the lock path if necessary
        //
        Stat stat = zk.retryOperation(new ZooKeeperOperation<Stat>() {
            @Override
            public Stat execute() throws KeeperException, InterruptedException {
                return zk.exists(lockPath, null);
            }
        });

        if (stat == null) {
            // We do not make use of ZkUtil.createPath (= recursive path creation) on purpose,
            // because if the parent path does not exist, this means the indexer does not exist,
            // and we do not want to create an indexer path (with null data) like that.
            try {
                zk.retryOperation(new ZooKeeperOperation<String>() {
                    @Override
                    public String execute() throws KeeperException, InterruptedException {
                        return zk.create(lockPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                });
            } catch (KeeperException.NodeExistsException e) {
                // ok, someone created it since we checked
            } catch (KeeperException.NoNodeException e) {
                throw new IndexerNotFoundException(indexerName);
            }
        }

        //
        // Take the actual lock
        //
        return ZkLock.lock(zk, indexerCollectionPathSlash + indexerName + "/lock");

    }


    @Override
    public String lockIndexer(String indexerName) throws ZkLockException, IndexerNotFoundException, InterruptedException,
            KeeperException, IndexerModelException {
        return lockIndexerInternal(indexerName, true);
    }

    @Override
    public void unlockIndexer(String lock) throws ZkLockException {
        ZkLock.unlock(zk, lock);
    }

    @Override
    public void unlockIndexer(String lock, boolean ignoreMissing) throws ZkLockException {
        ZkLock.unlock(zk, lock, ignoreMissing);
    }

    @Override
    public IndexerDefinition getIndexer(String name) throws IndexerNotFoundException {
        IndexerDefinition index = indexers.get(name);
        if (index == null) {
            throw new IndexerNotFoundException(name);
        }
        return index;
    }

    @Override
    public boolean hasIndexer(String name) {
        return indexers.containsKey(name);
    }

    @Override
    public IndexerDefinition getFreshIndexer(String name) throws InterruptedException, KeeperException, IndexerNotFoundException {
        return loadIndexer(name, false);
    }

    @Override
    public Collection<IndexerDefinition> getIndexers() {
        return new ArrayList<IndexerDefinition>(indexers.values());
    }

    @Override
    public Collection<IndexerDefinition> getIndexers(IndexerModelListener listener) {
        synchronized (indexers_lock) {
            registerListener(listener);
            return new ArrayList<IndexerDefinition>(indexers.values());
        }
    }

    private IndexerDefinition loadIndexer(String indexerName, boolean forCache)
            throws InterruptedException, KeeperException, IndexerNotFoundException {
        final String childPath = indexerCollectionPath + "/" + indexerName;
        final Stat stat = new Stat();
        
        byte[] data;
        try {
            if (forCache) {
                // do not retry, install watcher
                data = zk.getData(childPath, watcher, stat);
            } else {
                // do retry, do not install watcher
                data = zk.retryOperation(new ZooKeeperOperation<byte[]>() {
                    @Override
                    public byte[] execute() throws KeeperException, InterruptedException {
                        return zk.getData(childPath, false, stat);
                    }
                });
            }
        } catch (KeeperException.NoNodeException e) {
            throw new IndexerNotFoundException(indexerName);
        }

        IndexerDefinitionBuilder builder = IndexerDefinitionJsonSerDeser.INSTANCE.fromJsonBytes(data);
        builder.name(indexerName);
        builder.occVersion(stat.getVersion());
        return builder.build();
    }

    private void notifyListeners(List<IndexerModelEvent> events) {
        for (IndexerModelEvent event : events) {
            for (IndexerModelListener listener : listeners) {
                listener.process(event);
            }
        }
    }

    @Override
    public void registerListener(IndexerModelListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void unregisterListener(IndexerModelListener listener) {
        this.listeners.remove(listener);
    }

    private class IndexModelChangeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            try {
                if (NodeChildrenChanged.equals(event.getType()) && event.getPath().equals(indexerCollectionPath)) {
                    indexerCacheRefresher.triggerRefreshAllIndexes();
                } else if (NodeDataChanged.equals(event.getType()) && event.getPath().startsWith(indexerCollectionPathSlash)) {
                    String indexerName = event.getPath().substring(indexerCollectionPathSlash.length());
                    indexerCacheRefresher.triggerIndexToRefresh(indexerName);
                }
            } catch (Throwable t) {
                log.error("Indexer Model: error handling event from ZooKeeper. Event: " + event, t);
            }
        }
    }

    public class ConnectStateWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (stopped) {
                return;
            }

            if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
                // Each time the connection is established, we trigger refreshing, since the previous refresh
                // might have failed with a ConnectionLoss exception
                indexerCacheRefresher.triggerRefreshAllIndexes();
            }
        }
    }

    /**
     * Responsible for updating our internal cache of IndexerDefinition's. Should be triggered upon each related
     * change on ZK, as well as on ZK connection established, since this refresher simply fails on ZK connection
     * loss exceptions, rather than retrying.
     */
    private class IndexerCacheRefresher implements Runnable {
        private volatile Set<String> indexersToRefresh = new HashSet<String>();
        private volatile boolean refreshAllIndexers;
        private final Object refreshLock = new Object();
        private Thread thread;
        private final Object startedLock = new Object();
        private volatile boolean started = false;

        public synchronized void shutdown() throws InterruptedException {
            if (thread == null || !thread.isAlive()) {
                return;
            }

            thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() {
            // Upon startup, be sure to run a refresh of all indexes
            this.refreshAllIndexers = true;

            thread = new Thread(this, "Indexer model refresher");
            // Set as daemon thread: IndexerModel can be used in tools like the indexer admin CLI tools,
            // where we should not require explicit shutdown.
            thread.setDaemon(true);
            thread.start();
        }

        /**
         * Waits until the initial cache fill up happened.
         */
        public void waitUntilStarted() throws InterruptedException {
            synchronized (startedLock) {
                while (!started) {
                    startedLock.wait();
                }
            }
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    List<IndexerModelEvent> events = new ArrayList<IndexerModelEvent>();
                    try {
                        Set<String> indexersToRefresh = null;
                        boolean refreshAllIndexers = false;

                        synchronized (refreshLock) {
                            if (this.refreshAllIndexers || this.indexersToRefresh.isEmpty()) {
                                refreshAllIndexers = true;
                            } else {
                                indexersToRefresh = new HashSet<String>(this.indexersToRefresh);
                            }
                            this.refreshAllIndexers = false;
                            this.indexersToRefresh.clear();
                        }

                        if (refreshAllIndexers) {
                            synchronized (indexers_lock) {
                                refreshIndexers(events);
                            }
                        } else {
                            synchronized (indexers_lock) {
                                for (String indexerName : indexersToRefresh) {
                                    refreshIndexer(indexerName, events);
                                }
                            }
                        }

                        if (!started) {
                            started = true;
                            synchronized (startedLock) {
                                startedLock.notifyAll();
                            }
                        }
                    } finally {
                        // We notify the listeners here because we want to be sure events for every
                        // change are delivered, even if halfway through the refreshing we would have
                        // failed due to some error like a ZooKeeper connection loss
                        if (!events.isEmpty() && !stopped && !Thread.currentThread().isInterrupted()) {
                            notifyListeners(events);
                        }
                    }

                    synchronized (refreshLock) {
                        if (!this.refreshAllIndexers && this.indexersToRefresh.isEmpty()) {
                            refreshLock.wait();
                        }
                    }
                } catch (KeeperException.ConnectionLossException e) {
                    // we will be retriggered when the connection is back
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    log.error("Indexer Model Refresher: some exception happened.", t);
                }
            }
        }

        public void triggerIndexToRefresh(String indexName) {
            synchronized (refreshLock) {
                indexersToRefresh.add(indexName);
                refreshLock.notifyAll();
            }
        }

        public void triggerRefreshAllIndexes() {
            synchronized (refreshLock) {
                refreshAllIndexers = true;
                refreshLock.notifyAll();
            }
        }

        private void refreshIndexers(List<IndexerModelEvent> events) throws InterruptedException, KeeperException {
            List<String> indexerNames = zk.getChildren(indexerCollectionPath, watcher);

            Set<String> indexerNameSet = new HashSet<String>();
            indexerNameSet.addAll(indexerNames);

            // Remove indexers which no longer exist in ZK
            Iterator<String> currentIndexerNamesIt = indexers.keySet().iterator();
            while (currentIndexerNamesIt.hasNext()) {
                String indexerName = currentIndexerNamesIt.next();
                if (!indexerNameSet.contains(indexerName)) {
                    currentIndexerNamesIt.remove();
                    events.add(new IndexerModelEvent(INDEXER_DELETED, indexerName));
                }
            }

            // Add/update the other indexers
            for (String indexerName : indexerNames) {
                refreshIndexer(indexerName, events);
            }
        }

        /**
         * Adds or updates the given index to the internal cache.
         */
        private void refreshIndexer(final String indexerName, List<IndexerModelEvent> events)
                throws InterruptedException, KeeperException {
            try {
                IndexerDefinition indexer = loadIndexer(indexerName, true);

                IndexerDefinition oldIndexer = indexers.get(indexerName);

                if (oldIndexer != null && oldIndexer.getOccVersion() == indexer.getOccVersion()) {
                    // nothing changed
                } else {
                    final boolean isNew = oldIndexer == null;
                    indexers.put(indexerName, indexer);
                    events.add(new IndexerModelEvent(isNew ? INDEXER_ADDED : INDEXER_UPDATED, indexerName));
                }
            } catch (IndexerNotFoundException e) {
                Object oldIndexer = indexers.remove(indexerName);

                if (oldIndexer != null) {
                    events.add(new IndexerModelEvent(INDEXER_DELETED, indexerName));
                }
            }
        }
    }

    /**
     * Check the validity of an indexer name.
     * <p>
     * An indexer name can be any string of printable unicode characters that has a length greater than 0. Printable
     * characters in this context are considered to be anything that is not an ISO control character as defined by
     * {@link Character#isISOControl(int)}.
     * 
     * @param indexerName The name to validate
     */
    public static void validateIndexerName(String indexerName) {
        Preconditions.checkNotNull(indexerName);
        if (indexerName.length() == 0) {
            throw new IllegalArgumentException("Indexer name is empty");
        }
        for (int charIdx = 0; charIdx < indexerName.length(); charIdx++) {
            if (Character.isISOControl(indexerName.codePointAt(charIdx))) {
                throw new IllegalArgumentException("Indexer names may only consist of printable characters");
            }
        }
    }
}
